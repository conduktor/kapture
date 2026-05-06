# Releasing Kapture

The release pipeline is GitHub Actions on tag push (`v*`). It builds the Tauri app, relocates the forked librdkafka + Homebrew dylibs into `Kapture.app/Contents/Frameworks`, signs the updater artifact, and uploads everything to a GitHub Release alongside `latest.json`.

The in-app updater (`tauri-plugin-updater`) hits `https://github.com/sderosiaux/kapture/releases/latest/download/latest.json`, verifies the signature against the public key baked into `tauri.conf.json`, and prompts the user to install.

## One-time setup

1. Generate the updater signing keypair:

   ```bash
   pnpm tauri signer generate -w ~/.config/kapture/updater.key
   ```

   This produces a private key (`updater.key`) and prints the public key.

2. Paste the public key into `src-tauri/tauri.conf.json` under `plugins.updater.pubkey`, replacing `REPLACE_WITH_KAPTURE_UPDATER_PUBKEY`. Commit.

3. Add the private key to GitHub repo secrets:
   - `TAURI_SIGNING_PRIVATE_KEY` = contents of `updater.key`
   - `TAURI_SIGNING_PRIVATE_KEY_PASSWORD` = passphrase used at generation time

The private key never leaves your machine + GitHub secrets. **Treat the file as you would an SSH key.** Rotating it = users on older builds will refuse the update (good — they should download a fresh build manually).

### Optional: real macOS distribution (Developer ID + notarization)

Without these, releases ship ad-hoc-signed and Gatekeeper warns the user. Add three more secrets to enable real distribution:

- `APPLE_DEVELOPER_ID_APPLICATION` = e.g. `Developer ID Application: Your Name (TEAMID)`
- `APPLE_ID` = your Apple ID email
- `APPLE_APP_PASSWORD` = an app-specific password (generated at appleid.apple.com)
- `APPLE_TEAM_ID` = your team ID (10-char alphanumeric)

When all three Apple secrets are set, the release workflow runs `codesign --sign "${APPLE_DEVELOPER_ID_APPLICATION}"` instead of ad-hoc, then `xcrun notarytool submit` and `xcrun stapler staple`. The first release with notarization may take 5–15 minutes for Apple's service to finish.

Without these secrets the workflow still completes, but the relocate script logs a `WARN: ad-hoc signing only` line.

## Cutting a release

```bash
# Bump version in package.json + src-tauri/Cargo.toml + src-tauri/tauri.conf.json
git commit -am "release v0.2.0"
git tag v0.2.0
git push origin master --tags
```

GitHub Actions takes over from there. Watch the `Release` workflow — on green, the GitHub Release page has the `.app.tar.gz`, `.sig`, and `latest.json`.

## Local dry-run (macOS only)

```bash
pnpm librdkafka:build           # only on first run, or when the fork changes
pnpm tauri:release              # tauri build + relocate-macos-dylibs.sh
otool -L src-tauri/target/release/bundle/macos/Kapture.app/Contents/MacOS/kapture
# Verify: no /opt/homebrew/* paths.
```

## Why we relocate after Tauri

Tauri's `bundle.macOS.frameworks` will copy dylibs but doesn't rewrite `install_names`. Without rewrites, the loader looks up `/opt/homebrew/...` at runtime — fine on dev machines, broken everywhere else. `tools/relocate-macos-dylibs.sh` does the post-bundle surgery (copy → `install_name_tool -id` / `-change` → `codesign --force`).

The CI workflow also disables Tauri's auto-`createUpdaterArtifacts`. We sign manually after relocation; otherwise we'd be signing the un-relocated `.app` and the released bundle would mismatch its signature.
