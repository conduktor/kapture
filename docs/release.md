# Releasing Kapture

The release pipeline is GitHub Actions on tag push (`v*`). It builds the Tauri app, signs the updater artifact, and uploads everything to a GitHub Release alongside `latest.json`.

The in-app updater (`tauri-plugin-updater`) hits `https://github.com/conduktor/kapture/releases/latest/download/latest.json`, verifies the signature against the public key baked into `tauri.conf.json`, and prompts the user to install.

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

When all four Apple secrets are set, the release workflow runs `xcrun notarytool submit` and `xcrun stapler staple` on the bundled `.app`. The first notarization may take 5–15 minutes for Apple's service to finish.

Without these secrets the workflow still completes — Tauri ships an ad-hoc signed `.app` that Gatekeeper will warn on, but the in-app updater still verifies signatures with the embedded public key.

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
pnpm tauri build --bundles app
otool -L src-tauri/target/release/bundle/macos/Kapture.app/Contents/MacOS/kapture
# Verify: no /opt/homebrew/* paths.
```

## Why we sign manually

`createUpdaterArtifacts` is disabled in `tauri.conf.json` — the workflow tarballs `Kapture.app` and signs the archive itself with `tauri signer sign` after the (optional) notarization staple. This keeps the signed artifact and the file uploaded to the GitHub Release byte-identical, which is what `tauri-plugin-updater` validates against the public key.
