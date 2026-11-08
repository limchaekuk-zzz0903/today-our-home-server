#!/usr/bin/env bash
# 숫자섬 수학 탐험대(Elementaryschoolworksheets 저장소) 파일을 learn_static/ 으로 복사한다.
# 사용법: scripts/sync_learn.sh [학습지 저장소 경로]   (기본값: ../elementaryschoolworksheets)
# 경로가 없으면 GitHub에서 main 브랜치를 내려받는다.
set -euo pipefail
cd "$(dirname "$0")/.."
SRC="${1:-../elementaryschoolworksheets}"
TMP=""
if [ ! -f "$SRC/index.html" ]; then
  TMP="$(mktemp -d)"
  git clone --depth 1 https://github.com/limchaekuk-zzz0903/Elementaryschoolworksheets "$TMP/ws" >/dev/null
  SRC="$TMP/ws"
fi
REV="$(git -C "$SRC" rev-parse --short=12 HEAD 2>/dev/null || date +%Y%m%d%H%M%S)"
mkdir -p learn_static
for f in index.html manifest.webmanifest sw.js icon.svg icon-192.png icon-512.png; do
  cp "$SRC/$f" "learn_static/$f"
done
sed -i.bak "s/__BUILD__/$REV/" learn_static/sw.js && rm -f learn_static/sw.js.bak
echo "$REV" > learn_static/VERSION
[ -n "$TMP" ] && rm -r "$TMP"
echo "learn_static/ 갱신 완료 (학습지 $REV)"
