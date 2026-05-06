#!/usr/bin/env bash
# Relocate Homebrew + vendored dylibs into Kapture.app/Contents/Frameworks and
# rewrite their install names to @rpath/<basename>. Re-sign every modified
# binary because install_name_tool invalidates code signatures on Apple Silicon.
#
# Inputs:
#   $1 (optional): path to Kapture.app. Defaults to
#                  src-tauri/target/release/bundle/macos/Kapture.app
# Env overrides:
#   LIBRDKAFKA_DIR  default: vendor/librdkafka/install/lib
#   OPENSSL_DIR     default: /opt/homebrew/opt/openssl@3/lib
#   ZSTD_DIR        default: /opt/homebrew/opt/zstd/lib
#   CODESIGN_ID     default: -    (ad-hoc; CI sets this to a Developer ID)
#
# Why this script exists: Tauri's `bundle.macOS.frameworks` copies dylibs but
# does not rewrite install_names. Without rewrites, the loader looks up
# /opt/homebrew/... at runtime — fine on dev machines, broken on user machines.

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
APP_PATH="${1:-${REPO_ROOT}/src-tauri/target/release/bundle/macos/Kapture.app}"

if [[ ! -d "${APP_PATH}" ]]; then
  echo "error: ${APP_PATH} not found. Run 'pnpm tauri build' first." >&2
  exit 1
fi

LIBRDKAFKA_DIR="${LIBRDKAFKA_DIR:-${REPO_ROOT}/vendor/librdkafka/install/lib}"
OPENSSL_DIR="${OPENSSL_DIR:-/opt/homebrew/opt/openssl@3/lib}"
ZSTD_DIR="${ZSTD_DIR:-/opt/homebrew/opt/zstd/lib}"
CODESIGN_ID="${CODESIGN_ID:--}"

FRAMEWORKS="${APP_PATH}/Contents/Frameworks"
MACOS_DIR="${APP_PATH}/Contents/MacOS"
mkdir -p "${FRAMEWORKS}"

# Resolve the actual file behind a symlink. We always copy the real file and
# rename it to the canonical SONAME (libfoo.N.dylib).
resolve() {
  python3 -c 'import os, sys; print(os.path.realpath(sys.argv[1]))' "$1"
}

# Copy a single dylib into Frameworks, preserving the SONAME basename (eg.
# libzstd.1.dylib even if the underlying file is libzstd.1.5.7.dylib). librdkafka
# already references @rpath/libzstd.1.dylib so the basename must match.
copy_dylib() {
  local src="$1" target_basename="$2"
  local real
  real="$(resolve "${src}")"
  if [[ ! -f "${real}" ]]; then
    echo "error: source dylib not found: ${src} (resolved to ${real})" >&2
    exit 1
  fi
  cp -f "${real}" "${FRAMEWORKS}/${target_basename}"
  chmod u+w "${FRAMEWORKS}/${target_basename}"
}

echo ">> copying dylibs into ${FRAMEWORKS}"
copy_dylib "${LIBRDKAFKA_DIR}/librdkafka.1.dylib" "librdkafka.1.dylib"
copy_dylib "${OPENSSL_DIR}/libssl.3.dylib"        "libssl.3.dylib"
copy_dylib "${OPENSSL_DIR}/libcrypto.3.dylib"     "libcrypto.3.dylib"
copy_dylib "${ZSTD_DIR}/libzstd.1.dylib"          "libzstd.1.dylib"

# Set the install_name (id) of each dylib so dependents resolve via @rpath.
# Then rewrite every absolute /opt/homebrew/... reference to @rpath/<basename>.
# Note: librdkafka already references @rpath/libzstd.1.dylib (its build was
# configured that way); we still run the rewrite as a no-op safety net.
echo ">> rewriting install names"
for dylib in librdkafka.1.dylib libssl.3.dylib libcrypto.3.dylib libzstd.1.dylib; do
  local_path="${FRAMEWORKS}/${dylib}"
  install_name_tool -id "@rpath/${dylib}" "${local_path}"
done

# Exhaustive rewrite: anything pointing outside the system trust roots
# (/usr/lib, /System/Library) must end up either as @rpath/<basename of a file
# we shipped> OR fail loudly. This covers transitive deps (libssl → libcrypto
# via /opt/homebrew/Cellar/...), future Homebrew layouts, and the case where a
# new librdkafka version pulls in another dylib we didn't anticipate.
rewrite_deps() {
  local target="$1"
  local line dep base mapped
  while read -r line; do
    dep="$(echo "${line}" | awk '{print $1}')"
    case "${dep}" in
      ""|/usr/lib/*|/System/Library/*) continue ;;
      "${target}"|"@rpath/"*|"@executable_path/"*|"@loader_path/"*) continue ;;
    esac
    base="$(basename "${dep}")"
    # Map versioned basenames (libzstd.1.5.7.dylib) onto canonical SONAMEs
    # so both the dependency string and our shipped file basenames match.
    case "${base}" in
      libzstd.*.dylib)   mapped="libzstd.1.dylib" ;;
      libssl.*.dylib)    mapped="libssl.3.dylib" ;;
      libcrypto.*.dylib) mapped="libcrypto.3.dylib" ;;
      librdkafka*.dylib) mapped="librdkafka.1.dylib" ;;
      *)                 mapped="${base}" ;;
    esac
    if [[ ! -f "${FRAMEWORKS}/${mapped}" ]]; then
      echo "error: ${target} depends on ${dep} (basename ${mapped}) which is not bundled in ${FRAMEWORKS}" >&2
      exit 1
    fi
    install_name_tool -change "${dep}" "@rpath/${mapped}" "${target}"
  done < <(otool -L "${target}" | tail -n +2)
}

for dylib in librdkafka.1.dylib libssl.3.dylib libcrypto.3.dylib libzstd.1.dylib; do
  rewrite_deps "${FRAMEWORKS}/${dylib}"
done

# The main executable is whatever sits in MacOS/ (Tauri uses the productName).
# It already has @rpath/../Frameworks baked in via build.rs for release.
MAIN_BIN="$(find "${MACOS_DIR}" -maxdepth 1 -type f -perm -u+x | head -n 1)"
if [[ -z "${MAIN_BIN}" ]]; then
  echo "error: no executable found in ${MACOS_DIR}" >&2
  exit 1
fi
echo ">> rewriting main binary deps: ${MAIN_BIN}"
rewrite_deps "${MAIN_BIN}"

# Re-sign every binary we touched. install_name_tool invalidates the code
# signature, and macOS hard-rejects unsigned binaries on Apple Silicon.
# `set -e` already aborts on codesign failure, so silent failures are not
# possible here; a non-zero exit propagates out of the script.
echo ">> codesign (identity: ${CODESIGN_ID})"
if [[ "${CODESIGN_ID}" == "-" ]]; then
  echo "WARN: ad-hoc signing only — Gatekeeper will quarantine this build." >&2
  echo "      Set CODESIGN_ID to a Developer ID Application identity for" >&2
  echo "      distribution. Notarization (xcrun notarytool) is still required." >&2
fi
for dylib in librdkafka.1.dylib libssl.3.dylib libcrypto.3.dylib libzstd.1.dylib; do
  codesign --force --sign "${CODESIGN_ID}" "${FRAMEWORKS}/${dylib}"
done
codesign --force --sign "${CODESIGN_ID}" "${MAIN_BIN}"
# Re-seal the whole app last (ordering matters: nested binaries first).
codesign --force --sign "${CODESIGN_ID}" --deep "${APP_PATH}"

# Final invariant: scan EVERYTHING in the .app for any non-system absolute
# path. This catches future regressions (new transitive deps, helpers Tauri
# adds in nested bundles, etc.).
echo ">> verifying full .app has no /opt/homebrew references"
leaks="$(find "${APP_PATH}" -type f \( -perm -u+x -o -name '*.dylib' -o -name '*.so' \) -print0 \
  | xargs -0 -I{} sh -c 'otool -L "{}" 2>/dev/null | grep -E "(/opt/homebrew|/usr/local)" && echo "  IN: {}"' || true)"
if [[ -n "${leaks}" ]]; then
  echo "error: non-system absolute paths still present after relocation:" >&2
  echo "${leaks}" >&2
  exit 1
fi

echo ">> done. ${APP_PATH} is portable."
