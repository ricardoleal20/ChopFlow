#!/usr/bin/env bash
# One-time setup for signing + notarizing the macOS DMG in CI.
#
# Prerequisites (on this Mac, as the developer):
#   1. Apple Developer Program membership (https://developer.apple.com/programs)
#   2. A "Developer ID Application" certificate in your login keychain:
#      Xcode > Settings > Accounts (sign in with your Apple ID), then in the
#      developer portal (developer.apple.com/account) Certificates > + >
#      Developer ID Application. Download + install the .cer.
#      Verify with:  security find-identity -v -p codesigning
#
# What this script does:
#   - Exports your Developer ID certificate + private key to a .p12 (you choose
#     a password), base64-encodes it, and prints the value to paste into the
#     GitHub Actions secret APPLE_CERT_BASE64. APPLE_CERT_PASSWORD is the p12
#     password you choose here (also a GitHub secret).
#
# After running it, also create these GitHub secrets (repo Settings > Secrets
# and variables > Actions) for notarization (either pair A or B):
#   A. App-specific password (simplest):
#        APPLE_SIGNING_IDENTITY  "Developer ID Application: Your Name (TEAMID)"
#        APPLE_ID                your apple-id@example.com
#        APPLE_APP_PASSWORD      app-specific password from appleid.apple.com
#        APPLE_TEAM_ID           10-char team id (developer.apple.com > Membership)
#   B. App Store Connect API key (no 2FA surprises in CI):
#        APPLE_SIGNING_IDENTITY  (same as above)
#        APPLE_API_KEY_ID        key id from appstoreconnect.apple.com > Users
#                                and Access > Integrations > API Keys
#        APPLE_API_ISSUER_ID     issuer id, same page
#        APPLE_API_KEY           the contents of the downloaded .p8 file
#
set -euo pipefail

echo "Looking for a 'Developer ID Application' identity in your keychain…"
IDENTITIES=$(security find-identity -v -p codesigning 2>/dev/null | grep -i "Developer ID Application" || true)
if [ -z "$IDENTITIES" ]; then
  echo "No Developer ID Application identity found." >&2
  echo "Install the certificate first (see header of this script), then re-run." >&2
  exit 1
fi
echo "$IDENTITIES"

NAME=$(echo "$IDENTITIES" | head -1 | sed -E 's/^\s*[0-9]+\) "([^"]+)".*/\1/')
echo
echo "Using identity: $NAME"
echo

read -r -s -p "Choose a password for the exported .p12 (reuse it as the GitHub secret APPLE_CERT_PASSWORD): " P12PASS
echo
if [ -z "$P12PASS" ]; then
  echo "Password cannot be empty." >&2
  exit 1
fi

OUT="/tmp/ChopFlow_DeveloperID.p12"
security export -k "$HOME/Library/Keychains/login.keychain-db" \
  -t certs -f pkcs12 -P "$P12PASS" -o "$OUT" "$NAME" 2>/dev/null \
  || security export -k login.keychain -t certs -f pkcs12 -P "$P12PASS" -o "$OUT" "$NAME"

B64=$(base64 -i "$OUT")
echo
echo "Export OK. Paste this value into the GitHub secret  APPLE_CERT_BASE64:"
echo
echo "-----8<----- APPLE_CERT_BASE64 -----"
echo "$B64"
echo "-----8<-----"
echo
echo "And set  APPLE_CERT_PASSWORD  to: $P12PASS"
echo "Then add the notarization secrets (section A or B in the header of this file)."
echo "Temporary export left at: $OUT  (delete it after:  rm $OUT)"