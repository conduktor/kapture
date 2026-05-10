# Homebrew Cask for Kapture.
#
# This file is auto-bumped by .github/workflows/release.yml (update-cask
# job) on every tag push. Only `version`, `sha256` and KAPTURE_DMG_BASENAME
# are rewritten — the rest is human-edited.
#
# Install:
#   brew tap conduktor/kapture https://github.com/conduktor/kapture
#   brew install --cask kapture

cask "kapture" do
  version "0.1.0"
  sha256 "0000000000000000000000000000000000000000000000000000000000000000"

  # The bundler basename. Kept as a constant so the auto-bump workflow can
  # rewrite it deterministically without parsing Ruby string interpolation.
  KAPTURE_DMG_BASENAME = "Kapture_0.1.0_aarch64.dmg"

  url "https://github.com/conduktor/kapture/releases/download/v#{version}/#{KAPTURE_DMG_BASENAME}",
      verified: "github.com/conduktor/kapture/"

  name "Kapture"
  desc "Wireshark for Kafka — desktop traffic inspector"
  homepage "https://kapturekafka.dev"

  # No Intel build is shipped today. Drop or extend with on_intel/on_arm
  # blocks when an x86_64 target lands in the release workflow.
  depends_on arch: :arm64
  depends_on macos: ">= :sonoma"

  app "Kapture.app"

  zap trash: [
    "~/Library/Application Support/io.kapture.app",
    "~/Library/Caches/io.kapture.app",
    "~/Library/Preferences/io.kapture.app.plist",
    "~/Library/Saved Application State/io.kapture.app.savedState",
  ]
end
