"""지혜섬 탐험대 읽어 주기 음성(TTS) — /learn/api/tts, /learn/api/tts/status, /learn/api/tts/selftest.

Android 앱 WebView에는 Web Speech 음성이 없고, 아이폰은 무음 스위치가 Web Speech를 막는다.
그래서 서버가 글을 mp3로 만들어 주고 학습 페이지가 <audio>로 재생한다.
(today-our-home-server 저장소의 learning.py에서 옮겨 왔다. 학습 페이지에는 로그인이 없어 누구나 쓴다.)
"""
import asyncio
import base64
import hashlib
import json
import os
import re
import time
import urllib.parse
import urllib.request
from datetime import datetime
from typing import Optional

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse, Response

# 엔진 고르기(환경 변수):
#   TTS_ENGINE=edge   (기본) Microsoft Edge 읽어 주기 음성. 키가 필요 없다.
#   TTS_ENGINE=google Google Cloud Text-to-Speech. GOOGLE_TTS_API_KEY 필요(공식 유료 API).
#   TTS_ENGINE=gtranslate Google 번역 읽기 음성. 키가 필요 없다.
#   TTS_ENGINE=off    끔. 학습 페이지는 "부모님이 읽어 주기"로 대신한다.
# 기본 엔진이 실패하면 TTS_FALLBACK(기본 gtranslate)으로 한 번 더 만든다.
# (2026-11: Microsoft가 접속 규칙을 바꿔 edge-tts 7.2.7 이하는 403으로 막혔다. 7.3.1 이상 필요.)
TTS_ENGINE = os.environ.get("TTS_ENGINE", "edge").strip().lower()
TTS_FALLBACK = os.environ.get("TTS_FALLBACK", "gtranslate").strip().lower()
TTS_MAX_CHARS = 400
TTS_LANGS = {
    "ko-KR": {"edge": "ko-KR-SunHiNeural", "google": "ko-KR-Wavenet-A", "rate": "-8%", "speed": 0.92},
    "en-US": {"edge": "en-US-AnaNeural", "google": "en-US-Wavenet-F", "rate": "-15%", "speed": 0.85},
}
TTS_CACHE_DIR = os.environ.get("TTS_CACHE_DIR") or os.path.join(os.path.dirname(os.path.abspath(__file__)), ".tts_cache")
_TTS_MEM: dict = {}          # 최근 음성 (메모리)
_TTS_MEM_MAX = 300
_TTS_HITS: dict = {}         # IP별 최근 요청 시각 (남용 방지)
TTS_LAST = {"ok_at": None, "error": None, "error_at": None, "engine_used": None}  # 마지막 성공·실패 (점검용)
TTS_RATE = (240, 600)        # IP당 10분에 새 음성 240개까지 (캐시된 음성은 세지 않음)


async def _tts_edge(text: str, lang: str) -> bytes:
    import edge_tts  # 서버에만 필요. 테스트에서는 엔진을 바꿔 끼운다.
    conf = TTS_LANGS[lang]
    out = bytearray()
    async for chunk in edge_tts.Communicate(text, conf["edge"], rate=conf["rate"]).stream():
        if chunk.get("type") == "audio":
            out.extend(chunk["data"])
    return bytes(out)


async def _tts_google(text: str, lang: str) -> bytes:
    key = os.environ.get("GOOGLE_TTS_API_KEY", "")
    if not key:
        raise RuntimeError("GOOGLE_TTS_API_KEY 가 없어요")
    conf = TTS_LANGS[lang]
    body = json.dumps({
        "input": {"text": text},
        "voice": {"languageCode": lang, "name": conf["google"]},
        "audioConfig": {"audioEncoding": "MP3", "speakingRate": conf["speed"]},
    }).encode()
    req = urllib.request.Request(
        "https://texttospeech.googleapis.com/v1/text:synthesize",
        data=body, headers={"Content-Type": "application/json", "X-Goog-Api-Key": key}, method="POST")

    def call():
        with urllib.request.urlopen(req, timeout=20) as r:
            return base64.b64decode(json.loads(r.read())["audioContent"])
    return await asyncio.to_thread(call)


def _split_for(text: str, limit: int) -> list:
    """문장·쉼표·띄어쓰기 기준으로 limit 글자 이하 조각으로 나눈다."""
    parts, cur = [], ""
    for tok in re.split(r"(?<=[.!?。,])\s+|\s+", text):
        if not tok:
            continue
        cand = (cur + " " + tok).strip()
        if len(cand) <= limit:
            cur = cand
        else:
            if cur:
                parts.append(cur)
            while len(tok) > limit:
                parts.append(tok[:limit])
                tok = tok[limit:]
            cur = tok
    if cur:
        parts.append(cur)
    return parts


async def _tts_gtranslate(text: str, lang: str) -> bytes:
    """Google 번역 읽기 음성. 한 번에 200자 남짓까지라서 나눠 받아 mp3를 이어 붙인다(mp3 프레임은 이어 붙여도 재생된다)."""
    tl = lang.split("-")[0]

    def fetch(piece: str) -> bytes:
        q = urllib.parse.urlencode({"ie": "UTF-8", "client": "tw-ob", "tl": tl, "q": piece})
        req = urllib.request.Request("https://translate.google.com/translate_tts?" + q,
                                     headers={"User-Agent": "Mozilla/5.0", "Referer": "https://translate.google.com/"})
        with urllib.request.urlopen(req, timeout=15) as r:
            if r.status != 200 or "audio" not in (r.headers.get("Content-Type") or ""):
                raise RuntimeError(f"gtranslate {r.status} {r.headers.get('Content-Type')}")
            return r.read()

    out = bytearray()
    for piece in _split_for(text, 180):
        out.extend(await asyncio.to_thread(fetch, piece))
    return bytes(out)


TTS_ENGINES = {"edge": _tts_edge, "google": _tts_google, "gtranslate": _tts_gtranslate}


def tts_enabled() -> bool:
    return TTS_ENGINE in TTS_ENGINES


def _tts_key(text: str, lang: str) -> str:
    return hashlib.sha256(f"{TTS_ENGINE}|{lang}|{text}".encode()).hexdigest()


def _tts_allowed(ip: str) -> bool:
    limit, window = TTS_RATE
    now = time.time()
    hits = [t for t in _TTS_HITS.get(ip, []) if now - t < window]
    if len(hits) >= limit:
        _TTS_HITS[ip] = hits
        return False
    hits.append(now)
    _TTS_HITS[ip] = hits
    if len(_TTS_HITS) > 5000:
        _TTS_HITS.clear()
    return True


async def _make_voice(text: str, lang: str) -> Optional[bytes]:
    """기본 엔진 → 실패하면 예비 엔진. 실패 원인은 남긴다(Railway 로그·/learn/api/tts/status)."""
    engines = [TTS_ENGINE] + ([TTS_FALLBACK] if TTS_FALLBACK in TTS_ENGINES and TTS_FALLBACK != TTS_ENGINE else [])
    errors = []
    for name in engines:
        try:
            data = await asyncio.wait_for(TTS_ENGINES[name](text, lang), timeout=25)
            if data:
                TTS_LAST["engine_used"] = name
                return data
            errors.append(f"{name}: 빈 음성")
        except Exception as e:
            errors.append(f"{name}: {type(e).__name__}: {e}"[:300])
    msg = " | ".join(errors)[:600]
    TTS_LAST.update(error=msg, error_at=datetime.utcnow().isoformat(timespec="seconds"))
    print(f"[tts] 실패: {msg}", flush=True)
    return None


async def synthesize(text: str, lang: str, ip: str = "") -> Optional[bytes]:
    """글 → mp3. 캐시(메모리 → 디스크) 먼저 보고, 없으면 엔진으로 만든다. 실패하면 None."""
    key = _tts_key(text, lang)
    if key in _TTS_MEM:
        return _TTS_MEM[key]
    path = os.path.join(TTS_CACHE_DIR, key[:2], key + ".mp3")
    try:
        with open(path, "rb") as f:
            data = f.read()
        if data:
            _TTS_MEM[key] = data
            return data
    except OSError:
        pass
    if not _tts_allowed(ip):
        raise PermissionError("too many")
    data = await _make_voice(text, lang)
    if not data:
        return None
    TTS_LAST.update(ok_at=datetime.utcnow().isoformat(timespec="seconds"))
    if len(_TTS_MEM) >= _TTS_MEM_MAX:
        _TTS_MEM.pop(next(iter(_TTS_MEM)))
    _TTS_MEM[key] = data
    try:
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "wb") as f:
            f.write(data)
    except OSError:
        pass
    return data


def _audio_response(request: Request, data: bytes) -> Response:
    """mp3 응답. 아이폰(WebKit/AVFoundation)은 오디오를 'Range: bytes=0-1' 같은 부분 요청으로 먼저 받아 보고,
    서버가 206 부분 응답을 주지 않으면 재생하지 않는다. 그래서 Range 요청을 지원한다."""
    total = len(data)
    headers = {"Cache-Control": "public, max-age=31536000, immutable", "Accept-Ranges": "bytes"}
    rng = request.headers.get("range", "")
    m = re.match(r"^bytes=(\d*)-(\d*)$", rng.strip())
    if m and (m.group(1) or m.group(2)):
        if m.group(1):
            start = int(m.group(1))
            end = int(m.group(2)) if m.group(2) else total - 1
        else:  # bytes=-N : 마지막 N바이트
            start = max(0, total - int(m.group(2)))
            end = total - 1
        end = min(end, total - 1)
        if start >= total or start > end:
            return Response(status_code=416, headers={"Content-Range": f"bytes */{total}", "Accept-Ranges": "bytes"})
        part = data[start:end + 1]
        headers["Content-Range"] = f"bytes {start}-{end}/{total}"
        body = b"" if request.method == "HEAD" else part
        return Response(body, status_code=206, media_type="audio/mpeg",
                        headers={**headers, "Content-Length": str(len(part))})
    body = b"" if request.method == "HEAD" else data
    return Response(body, media_type="audio/mpeg", headers={**headers, "Content-Length": str(total)})


def _client_ip(request: Request) -> str:
    return (request.headers.get("x-forwarded-for", "").split(",")[0].strip()
            or (request.client.host if request.client else ""))


def register(app: FastAPI) -> None:
    @app.get("/learn/api/tts/status", include_in_schema=False)
    async def learn_tts_status():
        """학습 페이지가 열릴 때 확인: 서버 읽어 주기를 쓸 수 있는지."""
        return {"enabled": tts_enabled(), "engine": TTS_ENGINE if tts_enabled() else "off",
                "langs": list(TTS_LANGS), "max_chars": TTS_MAX_CHARS,
                "fallback": TTS_FALLBACK, "engine_used": TTS_LAST["engine_used"],
                "last_ok": TTS_LAST["ok_at"], "last_error": TTS_LAST["error"], "last_error_at": TTS_LAST["error_at"]}

    @app.get("/learn/api/tts/selftest", include_in_schema=False)
    async def learn_tts_selftest(request: Request):
        """서버에서 음성을 실제로 만들어 본다(캐시를 거치지 않음). 소리 문제를 점검할 때 브라우저로 열어 보면 된다."""
        if not tts_enabled():
            return {"ok": False, "engine": "off", "error": "TTS_ENGINE=off"}
        ip = _client_ip(request)
        if not _tts_allowed(ip):
            return JSONResponse({"ok": False, "error": "잠시 뒤에 다시 시도해 주세요"}, status_code=429)
        started = time.time()
        data = await _make_voice("안녕하세요. 소리 점검이에요.", "ko-KR")
        if not data:
            return {"ok": False, "engine": TTS_ENGINE, "fallback": TTS_FALLBACK, "error": TTS_LAST["error"],
                    "seconds": round(time.time() - started, 2)}
        return {"ok": True, "engine": TTS_ENGINE, "engine_used": TTS_LAST["engine_used"], "bytes": len(data),
                "head": (data or b"")[:4].hex(), "seconds": round(time.time() - started, 2)}

    @app.api_route("/learn/api/tts", methods=["GET", "HEAD"], include_in_schema=False)
    async def learn_tts(request: Request, t: str = "", l: str = "ko-KR"):
        """읽어 주기 음성(mp3). 누구나 쓸 수 있다(학습 페이지에는 로그인이 없음). 길이·언어·횟수 제한."""
        text = " ".join(t.split())
        if not tts_enabled():
            return JSONResponse({"detail": "읽어 주기 음성이 꺼져 있어요"}, status_code=503)
        if not text or len(text) > TTS_MAX_CHARS or l not in TTS_LANGS:
            return JSONResponse({"detail": "읽을 글이 올바르지 않아요"}, status_code=400)
        ip = _client_ip(request)
        try:
            data = await synthesize(text, l, ip)
        except PermissionError:
            return JSONResponse({"detail": "잠시 뒤에 다시 들어 주세요"}, status_code=429)
        if not data:
            return JSONResponse({"detail": "음성을 만들지 못했어요"}, status_code=503)
        return _audio_response(request, data)

