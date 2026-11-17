#!/bin/bash
set -euo pipefail

cd "$(dirname "$0")"

chmod +x hooks/*

# 清理旧版软链 (保留 .sample)
find .git/hooks -maxdepth 1 -type l -delete

git config core.hooksPath hooks

echo "Git hooks installed (core.hooksPath=hooks)."
