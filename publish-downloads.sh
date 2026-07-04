#!/usr/bin/env bash
# publish-downloads.sh — push the built desktop binaries to https://siliqs.net/downloads/
#
# NO AWS keys in GitHub. GitHub Actions only BUILDS (it uploads the three binaries as
# workflow artifacts on every tag/manual run). You publish from YOUR machine, where the
# `siliqs` AWS profile already lives (1Password) — the credentials never leave your laptop.
#
# Usage:
#   ./publish-downloads.sh <tag>     # e.g. v0.3.4 — pulls that build's CI artifacts, uploads to S3
#   ./publish-downloads.sh --local   # uploads the binaries already in ./out (a local build)
#
# Needs: gh (GitHub CLI, logged in) + aws CLI with the `siliqs` profile configured.
set -euo pipefail

PROFILE=siliqs
REGION=ap-northeast-1
BUCKET=siliqs-net-site-test
CF_DIST=EICN3L9GA5F65
REPO=siliqs/siliqs-mesh-bridge
FILES=(
  siliqs-mesh-bridge-macos-arm64.dmg
  siliqs-mesh-bridge-windows-x86_64.exe
  siliqs-mesh-bridge-linux-x86_64
)

src_dir="$(mktemp -d)"
trap 'rm -rf "$src_dir"' EXIT

if [ "${1:-}" = "--local" ]; then
  echo "• using locally built binaries in ./out"
  cp out/* "$src_dir/"
else
  tag="${1:?usage: ./publish-downloads.sh <tag>|--local}"
  echo "• finding the 'Build desktop apps' run for $tag …"
  run_id="$(gh run list --repo "$REPO" --workflow "Build desktop apps" \
             --branch "$tag" --limit 1 --json databaseId --jq '.[0].databaseId')"
  [ -n "$run_id" ] || { echo "no CI run found for tag $tag (push the tag first, let it build)"; exit 1; }
  echo "• downloading artifacts from run $run_id …"
  gh run download "$run_id" --repo "$REPO" --dir "$src_dir"
fi

echo "• uploading to s3://$BUCKET/downloads/ …"
for f in "${FILES[@]}"; do
  path="$(find "$src_dir" -type f -name "$f" | head -1)"
  [ -n "$path" ] || { echo "  ✗ missing $f"; exit 1; }
  aws --profile "$PROFILE" --region "$REGION" s3 cp "$path" "s3://$BUCKET/downloads/$f" \
    --content-type application/octet-stream --cache-control "public, max-age=300"
done

echo "• invalidating CloudFront …"
aws --profile "$PROFILE" --region "$REGION" cloudfront create-invalidation \
  --distribution-id "$CF_DIST" --paths "/downloads/*" \
  --query 'Invalidation.{Id:Id,Status:Status}' --output text

echo "✓ published — https://siliqs.net/downloads/"
