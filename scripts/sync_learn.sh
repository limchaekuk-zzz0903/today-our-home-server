#!/usr/bin/env bash
# 숫자섬 수학 탐험대(servicemap/math-adventure) 파일을 learn_static/ 으로 복사한다.
# 사용법: scripts/sync_learn.sh [servicemap 저장소 경로]   (기본값: ../servicemap)
# 경로가 없으면 GitHub에서 main 브랜치를 내려받는다.
set -euo pipefail
cd "$(dirname "$0")/.."
SRC="${1:-../servicemap}"
TMP=""
if [ ! -d "$SRC/math-adventure" ]; then
  TMP="$(mktemp -d)"
  git clone --depth 1 https://github.com/limchaekuk-zzz0903/servicemap "$TMP/servicemap" >/dev/null
  SRC="$TMP/servicemap"
fi
REV="$(git -C "$SRC" rev-parse --short=12 HEAD 2>/dev/null || date +%Y%m%d%H%M%S)"
mkdir -p learn_static
for f in index.html manifest.webmanifest sw.js icon.svg icon-192.png icon-512.png; do
  cp "$SRC/math-adventure/$f" "learn_static/$f"
done
sed -i.bak "s/__BUILD__/$REV/" learn_static/sw.js && rm -f learn_static/sw.js.bak
echo "$REV" > learn_static/VERSION
[ -n "$TMP" ] && rm -r "$TMP"
echo "learn_static/ 갱신 완료 (servicemap $REV)"
