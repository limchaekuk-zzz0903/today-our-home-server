#!/bin/sh
# 지혜섬 탐험대(수학·국어·영어·과학 학습 웹앱)를 GitHub 저장소에서 받아 이 폴더를 최신으로 맞춘다.
# 원본: https://github.com/limchaekuk-zzz0903/Elementaryschoolworksheets (main)
# 서버는 이 폴더의 파일을 /learn/ 아래에서 그대로 서빙한다. 받은 뒤 `railway up`으로 배포.
set -eu
cd "$(dirname "$0")"
REF="${1:-main}"
BASE="https://raw.githubusercontent.com/limchaekuk-zzz0903/Elementaryschoolworksheets/$REF"
for f in index.html catalog.json manifest.webmanifest sw.js icon.svg icon-192.png icon-512.png; do
  curl -fsSL "$BASE/$f" -o "$f.tmp"
  mv "$f.tmp" "$f"
done
SHA=$(curl -fsSL "https://api.github.com/repos/limchaekuk-zzz0903/Elementaryschoolworksheets/commits/$REF" | sed -n 's/^  "sha": "\([0-9a-f]*\)".*/\1/p' | head -1)
echo "${SHA:-unknown}" > VERSION
echo "학습 앱을 $REF (${SHA:-unknown})로 맞췄어요."
