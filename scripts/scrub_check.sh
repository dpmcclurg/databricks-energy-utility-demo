#!/usr/bin/env bash
# Fails if files that would be pushed contain workspace-specific or personal values. Run before every push.
set -euo pipefail
cd "$(dirname "$0")/.."
patterns=(
  'cloud\.databricks\.com' 'azuredatabricks\.net' 'databricksapps\.com'
  '@databricks\.com' 'dapi[0-9a-f]{20,}' 'serverless_stable_' 'ep-[a-z]+-[a-z]+-[a-z0-9]{8}'
  '\?o=[0-9]{6,}' 'BEGIN (RSA|OPENSSH) PRIVATE KEY'
)
files=$(git ls-files --cached --others --exclude-standard | grep --invert-match '^scripts/scrub_check.sh$')
fail=0
for p in "${patterns[@]}"; do
  if hits=$(echo "$files" | xargs grep --line-number --binary-files=without-match --extended-regexp "$p" 2>/dev/null); then
    echo "BLOCKED pattern '$p':"; echo "$hits" | head -5; fail=1
  fi
done
if [ "$fail" -eq 0 ]; then echo "scrub check passed"; fi
exit $fail
