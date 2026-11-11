"""
학습 페이지(지혜섬 탐험대) 연동

흐름
  1) 앱이 인증 헤더(X-Device-ID, X-Device-Secret)와 함께 POST /api/learning/session 호출
     → 5분 동안 한 번만 쓸 수 있는 입장권(ticket)이 담긴 URL을 받음
  2) 앱이 그 URL을 WebView로 염 → GET /learn/?ticket=...
     → 입장권 확인 후 진도 저장용 쿠키(toh_learn, 30일)를 심고 /learn/ 으로 이동
  3) 학습 페이지가 /learn/api/progress 로 별·크리스탈 진도를 불러오고 저장(쿠키 인증)
  4) 앱은 GET /api/learning/subjects 로 과목 목록(수학·국어·영어·과학)을 받아 과목별 버튼을 만들고,
     GET /api/learning/family-progress 로 가족 구성원별·과목별 학습 현황을 조회

과목·학년·화 구성은 learn_static/catalog.json(학습지 저장소가 만들어 주는 파일)에서 읽는다.
과목이나 화가 늘어나도 scripts/sync_learn.sh 로 파일만 갱신하면 서버 코드는 그대로 둔다.

학습 페이지 자체에는 회원·로그인 개념이 없다. /learn/ 은 누구에게나 열린다(입장권이 없거나,
이미 썼거나, 만료돼도 페이지는 그대로 열림). 쿠키는 오늘우리집 회원의 진도를 서버에 저장하는
데에만 쓰이고, 쿠키가 없으면 학습 페이지는 기기(브라우저) 안에만 진도를 저장한다.
→ "회원만 볼 수 있음" 화면이 다시 생기지 않도록 tests/test_learning.py 가 지킨다.

/learn 아래 경로는 기기 인증 미들웨어에서 제외된다. 진도 API(/learn/api/progress)만 쿠키를 확인한다.
"""
import asyncio
import base64
import hashlib
import json
import time
import urllib.request
import os
import re
import secrets
from datetime import datetime, timedelta
from typing import Optional

from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import FileResponse, JSONResponse, RedirectResponse, Response

STATIC_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "learn_static")
STATIC_FILES = {
    "index.html": "text/html; charset=utf-8",
    "catalog.json": "application/json",
    "manifest.webmanifest": "application/manifest+json",
    "sw.js": "text/javascript",
    "icon.svg": "image/svg+xml",
    "icon-192.png": "image/png",
    "icon-512.png": "image/png",
}
COOKIE = "toh_learn"
COOKIE_PATH = "/learn"
TICKET_TTL = timedelta(minutes=5)
SESSION_TTL = timedelta(days=30)
MAX_BODY = 64_000


def _load_catalog() -> dict:
    """learn_static/catalog.json: 과목 → 학년 → 지역(대장 문제 크리스탈) → 화."""
    try:
        with open(os.path.join(STATIC_DIR, "catalog.json"), encoding="utf-8") as f:
            cat = json.load(f)
        if isinstance(cat.get("subjects"), list) and cat["subjects"]:
            return cat
    except (OSError, ValueError):
        pass
    return {"title": "지혜섬 탐험대", "lessons_total": 76, "crystals_total": 22, "subjects": []}


CATALOG = _load_catalog()
TOTAL_LESSONS = CATALOG.get("lessons_total", 76)
TOTAL_CRYSTALS = CATALOG.get("crystals_total", 22)
# 화 id·지역 id → 과목 id
LESSON_SUBJECT = {}
REGION_SUBJECT = {}
VALID_STARTS = {"home", "daily", "trophy"}
for _s in CATALOG["subjects"]:
    VALID_STARTS.add(_s["id"])
    for _g in _s.get("grades", []):
        VALID_STARTS.add(_g["start"])
        VALID_STARTS.add(f"speed-{_s['id']}-g{_g['g']}")
        for _r in _g.get("regions", []):
            REGION_SUBJECT[_r["id"]] = _s["id"]
            VALID_STARTS.add(_r["boss"])
            for _l in _r.get("lessons", []):
                LESSON_SUBJECT[_l["id"]] = _s["id"]
                VALID_STARTS.add(_l["id"])


def _valid_start(start: Optional[str]) -> bool:
    if not start:
        return False
    if VALID_STARTS - {"home", "daily", "trophy"}:
        return start in VALID_STARTS
    # catalog.json 이 없을 때의 예전 규칙
    return bool(_LESSON_RE.match(start) or start in ("home", "math", "kor", "eng", "sci", "daily", "trophy")
                or re.match(r"^(?:(?:kor|eng|sci)-)?g[1-6]$", start))


def _level(xp: int) -> int:
    """학습 페이지와 같은 규칙: 레벨 n 이 되려면 경험치 20·n·(n+1) 이 필요."""
    n = 1
    while xp >= 20 * n * (n + 1):
        n += 1
    return n


def _subjects_meta() -> list:
    """앱의 과목 메뉴용 목록 (화 목록은 빼고 학년·지역 이름까지)."""
    out = []
    for s in CATALOG["subjects"]:
        out.append({
            "id": s["id"], "name": s["name"], "icon": s.get("icon", ""), "span": s.get("span", ""),
            "desc": s.get("desc", ""), "start": s["id"],
            "lessons_total": s.get("lessons_total", 0), "crystals_total": s.get("crystals_total", 0),
            "grades": [{"g": g["g"], "name": f"{g['g']}학년", "island": g.get("island", ""), "focus": g.get("focus", ""),
                        "start": g["start"], "regions": [r["name"] for r in g.get("regions", [])]}
                       for g in s.get("grades", [])],
        })
    return out
_LESSON_RE = re.compile(r"^[a-z]{2,10}-[0-9a-z]{1,8}$")
_REGION_RE = re.compile(r"^[a-z]{2,10}$")

INIT_STMTS = [
    """CREATE TABLE IF NOT EXISTS learning_tickets (
        ticket_hash TEXT PRIMARY KEY,
        device_id   TEXT NOT NULL,
        start       TEXT,
        expires_at  TEXT NOT NULL,
        used        INTEGER DEFAULT 0
    )""",
    """CREATE TABLE IF NOT EXISTS learning_sessions (
        token_hash  TEXT PRIMARY KEY,
        device_id   TEXT NOT NULL,
        created_at  TEXT NOT NULL,
        expires_at  TEXT NOT NULL
    )""",
    """CREATE TABLE IF NOT EXISTS learning_progress (
        device_id   TEXT PRIMARY KEY,
        data        TEXT NOT NULL,
        updated_at  TEXT NOT NULL
    )""",
]

# ── 읽어 주기 음성(TTS) ────────────────────────────────────────────────────────
# Android 앱 화면(WebView)에는 웹 음성 합성이 없어서 학습 페이지가 소리를 낼 수 없다.
# 그래서 서버가 글을 mp3로 만들어 주고, 학습 페이지가 <audio>로 재생한다(앱 수정 불필요).
#   TTS_ENGINE=edge   (기본) Microsoft Edge 읽어 주기 음성. 키가 필요 없다.
#   TTS_ENGINE=google Google Cloud Text-to-Speech. GOOGLE_TTS_API_KEY 필요(공식 유료 API).
#   TTS_ENGINE=off    끔. 학습 페이지는 "부모님이 읽어 주기"로 대신한다.
TTS_ENGINE = os.environ.get("TTS_ENGINE", "edge").strip().lower()
TTS_MAX_CHARS = 400
TTS_LANGS = {
    "ko-KR": {"edge": "ko-KR-SunHiNeural", "google": "ko-KR-Wavenet-A", "rate": "-8%", "speed": 0.92},
    "en-US": {"edge": "en-US-AnaNeural", "google": "en-US-Wavenet-F", "rate": "-15%", "speed": 0.85},
}
TTS_CACHE_DIR = os.environ.get("TTS_CACHE_DIR") or os.path.join(os.path.dirname(os.path.abspath(__file__)), ".tts_cache")
_TTS_MEM: dict = {}          # 최근 음성 (메모리)
_TTS_MEM_MAX = 300
_TTS_HITS: dict = {}         # IP별 최근 요청 시각 (남용 방지)
TTS_LAST = {"ok_at": None, "error": None, "error_at": None}  # 마지막 성공·실패 (점검용)
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


TTS_ENGINES = {"edge": _tts_edge, "google": _tts_google}


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
    try:
        data = await asyncio.wait_for(TTS_ENGINES[TTS_ENGINE](text, lang), timeout=25)
    except Exception as e:  # 엔진 실패 원인을 남긴다(Railway 로그·/learn/api/tts/status 에서 확인)
        msg = f"{type(e).__name__}: {e}"[:300] or type(e).__name__
        TTS_LAST.update(error=msg, error_at=datetime.utcnow().isoformat(timespec="seconds"))
        print(f"[tts] {TTS_ENGINE} 실패: {msg}", flush=True)
        return None
    if not data:
        TTS_LAST.update(error="빈 음성", error_at=datetime.utcnow().isoformat(timespec="seconds"))
        print(f"[tts] {TTS_ENGINE} 빈 음성", flush=True)
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


def _utcnow() -> datetime:
    return datetime.utcnow()


def _iso(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%dT%H:%M:%S")


def _hash(token: str) -> str:
    return hashlib.sha256(token.encode()).hexdigest()


def _is_https(request: Request) -> bool:
    proto = request.headers.get("x-forwarded-proto", "").split(",")[0].strip()
    return (proto or request.url.scheme) == "https"


def _public_base(request: Request) -> str:
    proto = request.headers.get("x-forwarded-proto", "").split(",")[0].strip() or request.url.scheme
    host = request.headers.get("x-forwarded-host") or request.headers.get("host") or request.url.netloc
    return f"{proto}://{host}"


_BADGE_RE = re.compile(r"^[a-z0-9-]{1,20}$")
_DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")
_SPEED_RE = re.compile(r"^(?:math|kor|eng|sci)-[1-6]$")


def _int(v, hi: int) -> int:
    try:
        n = int(v)
    except (TypeError, ValueError):
        return 0
    return n if 0 <= n <= hi else 0


def _clean_progress(raw) -> dict:
    """앱이 보낸 진도를 검증해서 정해진 모양으로만 저장한다."""
    if not isinstance(raw, dict):
        raw = {}
    stars = {}
    for k, v in list((raw.get("stars") or {}).items())[:200]:
        if isinstance(k, str) and _LESSON_RE.match(k):
            n = _int(v, 3)
            if n > 0:
                stars[k] = n
    crystals = {}
    for k, v in list((raw.get("crystals") or {}).items())[:60]:
        if isinstance(k, str) and _REGION_RE.match(k) and v is True:
            crystals[k] = True
    name = raw.get("name")
    name = name.strip()[:20] if isinstance(name, str) else ""

    def dated(key, key_re, limit):
        out = {}
        src = raw.get(key)
        for k, v in list((src if isinstance(src, dict) else {}).items())[:limit]:
            if isinstance(k, str) and key_re.match(k) and isinstance(v, (str, int)) and not isinstance(v, bool):
                out[k] = v if isinstance(v, int) else str(v)[:10]
        return out

    speed = {}
    src = raw.get("speed")
    for k, v in list((src if isinstance(src, dict) else {}).items())[:40]:
        if isinstance(k, str) and _SPEED_RE.match(k):
            n = _int(v, 200)
            if n:
                speed[k] = n
    days = raw.get("days")
    days = sorted({d for d in (days if isinstance(days, list) else [])[-400:] if isinstance(d, str) and _DATE_RE.match(d)})
    return {
        "stars": stars,
        "crystals": crystals,
        "name": name,
        # 성취 장치(경험치·배지·메달·기록). 여러 기기에서 저장돼도 줄지 않게 합친다.
        "xp": _int(raw.get("xp"), 10_000_000),
        "maxCombo": _int(raw.get("maxCombo"), 1000),
        "correct": _int(raw.get("correct"), 10_000_000),
        "cleared": _int(raw.get("cleared"), 1_000_000),
        "badges": dated("badges", _BADGE_RE, 60),
        "medals": dated("medals", _LESSON_RE, 200),
        "daily": dated("daily", _DATE_RE, 400),
        "speed": speed,
        "days": days,
    }


def _merge(old: dict, new: dict) -> dict:
    """여러 번 저장돼도 진도가 줄지 않도록 별·기록은 큰 값, 크리스탈·배지·메달은 하나라도 있으면 유지."""
    stars = dict(old.get("stars", {}))
    for k, v in new["stars"].items():
        stars[k] = max(stars.get(k, 0), v)
    crystals = {**old.get("crystals", {}), **new["crystals"]}
    speed = dict(old.get("speed", {}))
    for k, v in new.get("speed", {}).items():
        speed[k] = max(speed.get(k, 0), v)
    merged = {
        "stars": stars,
        "crystals": crystals,
        "name": new["name"] or old.get("name", ""),
        "speed": speed,
        "days": sorted(set(old.get("days", [])) | set(new.get("days", [])))[-400:],
    }
    for k in ("xp", "maxCombo", "correct", "cleared"):
        merged[k] = max(old.get(k, 0) or 0, new.get(k, 0) or 0)
    for k in ("badges", "medals", "daily"):
        merged[k] = {**new.get(k, {}), **old.get(k, {})}
    return merged


def _summary(progress: Optional[dict]) -> dict:
    p = progress or {"stars": {}, "crystals": {}}
    stars = p.get("stars", {})
    crystals = p.get("crystals", {})
    subjects = {}
    for s in CATALOG["subjects"]:
        sid = s["id"]
        ls = {k: v for k, v in stars.items() if LESSON_SUBJECT.get(k) == sid}
        subjects[sid] = {
            "name": s["name"],
            "lessons_done": sum(1 for v in ls.values() if v > 0),
            "lessons_total": s.get("lessons_total", 0),
            "stars_total": sum(ls.values()),
            "stars_max": s.get("lessons_total", 0) * 3,
            "crystals": sorted(k for k, v in crystals.items() if v and REGION_SUBJECT.get(k) == sid),
            "crystals_total": s.get("crystals_total", 0),
        }
    xp = p.get("xp", 0) or 0
    return {
        "lessons_done": sum(1 for v in stars.values() if v > 0),
        "stars_total": sum(stars.values()),
        "crystals": sorted(k for k, v in p.get("crystals", {}).items() if v),
        "xp": xp,
        "level": _level(xp),
        "study_days": len(p.get("days", []) or []),
        "badges": len(p.get("badges", {}) or {}),
        "medals": len(p.get("medals", {}) or {}),
        "subjects": subjects,
    }


_DB = None


async def init_tables() -> None:
    """main.py 의 startup 에서 호출: 학습용 테이블 생성."""
    for stmt in INIT_STMTS:
        try:
            await _DB.execute(stmt)
        except Exception as e:  # noqa: BLE001
            print(f"learning init warning: {e}")


def register(app: FastAPI, DB, upsert_sql) -> None:
    """main.py에서 호출: 라우트를 등록한다. (테이블은 init_tables)

    upsert_sql(table, key, cols) → INSERT ... ON CONFLICT ... DO UPDATE 문 (PG/SQLite 공용 '?' 표기)
    """

    global _DB
    _DB = DB

    async def _device_from_cookie(request: Request) -> Optional[str]:
        token = request.cookies.get(COOKIE)
        if not token:
            return None
        row = await DB.fetchrow(
            "SELECT device_id, expires_at FROM learning_sessions WHERE token_hash = ?", _hash(token)
        )
        if not row or row["expires_at"] < _iso(_utcnow()):
            return None
        # 기기가 로그아웃(시크릿 삭제)됐으면 학습 세션도 무효
        sec = await DB.fetchrow("SELECT device_id FROM device_secrets WHERE device_id = ?", row["device_id"])
        return row["device_id"] if sec else None

    async def _load_progress(device_id: str) -> Optional[dict]:
        row = await DB.fetchrow("SELECT data FROM learning_progress WHERE device_id = ?", device_id)
        if not row:
            return None
        try:
            return json.loads(row["data"])
        except (TypeError, ValueError):
            return None

    # ── 앱이 호출하는 API (기기 인증 미들웨어 통과 후) ─────────────────────────

    @app.post("/api/learning/session")
    async def create_learning_session(request: Request, start: Optional[str] = None):
        """학습 페이지를 열 1회용 URL 발급.
        start 로 시작 화면을 지정: kor(과목 페이지), eng-g4(과목·학년 지도), frac-1(화), daily(오늘의 도전) 등.
        생략하면 과목 선택 첫 화면."""
        device_id = request.headers.get("X-Device-ID", "")
        # 기기 비밀값은 인증 미들웨어가 이미 확인했다. devices 행이 없더라도 학습 페이지는 열 수 있어야 한다.
        if not _valid_start(start):
            start = None
        ticket = secrets.token_urlsafe(32)
        expires = _utcnow() + TICKET_TTL
        await DB.execute(
            "INSERT INTO learning_tickets (ticket_hash, device_id, start, expires_at, used) VALUES (?, ?, ?, ?, 0)",
            _hash(ticket), device_id, start, _iso(expires),
        )
        # 오래된 입장권·세션 정리
        await DB.execute("DELETE FROM learning_tickets WHERE expires_at < ?", _iso(_utcnow() - timedelta(days=1)))
        await DB.execute("DELETE FROM learning_sessions WHERE expires_at < ?", _iso(_utcnow()))
        return {
            "url": f"{_public_base(request)}/learn/?ticket={ticket}",
            "expires_in": int(TICKET_TTL.total_seconds()),
            # 입장권 없이 여는 주소. 이 주소로 열어도 학습은 되고, 진도는 기기 안에만 저장된다.
            "public_url": f"{_public_base(request)}/learn/" + (f"#{start}" if start else ""),
        }

    @app.get("/api/learning/subjects")
    async def learning_subjects():
        """과목 목록. 앱은 이걸로 과목별 버튼을 만들고, 버튼마다 session?start=<id> 로 연다."""
        return {"title": CATALOG.get("title", "지혜섬 탐험대"), "lessons_total": TOTAL_LESSONS,
                "crystals_total": TOTAL_CRYSTALS, "subjects": _subjects_meta()}

    @app.get("/api/learning/progress")
    async def my_learning_progress(request: Request):
        """내 학습 진도 요약 (앱 홈 화면 카드 등에 사용)."""
        device_id = request.headers.get("X-Device-ID", "")
        return {"device_id": device_id, **_summary(await _load_progress(device_id))}

    @app.get("/api/learning/family-progress")
    async def family_learning_progress(request: Request):
        """같은 가족 그룹 구성원들의 학습 현황."""
        device_id = request.headers.get("X-Device-ID", "")
        device = await DB.fetchrow("SELECT family_id FROM devices WHERE id = ?", device_id)
        if not device or not device.get("family_id"):
            raise HTTPException(status_code=400, detail="가족 그룹에 속해 있지 않아요")
        members = await DB.fetch(
            "SELECT d.id, d.user_name, lp.data, lp.updated_at FROM devices d"
            " LEFT JOIN learning_progress lp ON lp.device_id = d.id WHERE d.family_id = ?",
            device["family_id"],
        )
        result = []
        for m in members:
            try:
                prog = json.loads(m["data"]) if m.get("data") else None
            except (TypeError, ValueError):
                prog = None
            result.append({
                "device_id": m["id"],
                "user_name": m["user_name"],
                "updated_at": m.get("updated_at"),
                **_summary(prog),
            })
        return {
            "total_lessons": TOTAL_LESSONS,
            "total_crystals": TOTAL_CRYSTALS,
            "subjects": [{"id": s["id"], "name": s["name"], "icon": s["icon"], "lessons_total": s["lessons_total"],
                          "crystals_total": s["crystals_total"]} for s in _subjects_meta()],
            "members": result,
        }

    # ── 학습 페이지 (쿠키 인증) ──────────────────────────────────────────────

    @app.get("/learn", include_in_schema=False)
    async def learn_redirect(request: Request):
        q = f"?{request.url.query}" if request.url.query else ""
        return RedirectResponse(f"/learn/{q}", status_code=307)

    def _page():
        return FileResponse(
            os.path.join(STATIC_DIR, "index.html"),
            media_type="text/html; charset=utf-8",
            headers={"Cache-Control": "no-cache", "X-Frame-Options": "SAMEORIGIN"},
        )

    @app.get("/learn/", include_in_schema=False)
    async def learn_page(request: Request, ticket: Optional[str] = None, start: Optional[str] = None):
        """학습 페이지. 누구나 열 수 있다. 유효한 입장권이 있으면 진도 저장용 쿠키만 덧붙인다."""
        if ticket is None:
            # /learn/?start=kor 처럼 열면 시작 화면으로 보내 준다
            if start and _valid_start(start):
                return RedirectResponse(f"/learn/#{start}", status_code=303)
            return _page()
        row = await DB.fetchrow(
            "SELECT device_id, start, expires_at, used FROM learning_tickets WHERE ticket_hash = ?", _hash(ticket)
        )
        start_screen = (row.get("start") if row else None) or (start if _valid_start(start) else None)
        # 입장권은 URL에서 지운다(새로고침·공유 때 다시 쓰이지 않도록). 시작 화면은 # 뒤로 넘긴다.
        resp = RedirectResponse("/learn/" + (f"#{start_screen}" if start_screen else ""), status_code=303)
        if row and not row["used"] and row["expires_at"] >= _iso(_utcnow()):
            await DB.execute("UPDATE learning_tickets SET used = 1 WHERE ticket_hash = ?", _hash(ticket))
            token = secrets.token_urlsafe(32)
            now = _utcnow()
            await DB.execute(
                "INSERT INTO learning_sessions (token_hash, device_id, created_at, expires_at) VALUES (?, ?, ?, ?)",
                _hash(token), row["device_id"], _iso(now), _iso(now + SESSION_TTL),
            )
            resp.set_cookie(
                COOKIE, token, max_age=int(SESSION_TTL.total_seconds()), path=COOKIE_PATH,
                httponly=True, samesite="lax", secure=_is_https(request),
            )
        # 이미 쓴(또는 만료된) 입장권: WebView가 같은 주소를 다시 열었을 때. 이미 받은 쿠키가 있으면
        # 그대로 진도가 저장되고, 없으면 기기 안에만 저장된다. 어느 쪽이든 페이지는 막지 않는다.
        return resp

    @app.get("/learn/api/progress", include_in_schema=False)
    async def learn_get_progress(request: Request):
        device_id = await _device_from_cookie(request)
        if not device_id:
            return JSONResponse({"detail": "로그인이 필요해요"}, status_code=401)
        dev = await DB.fetchrow("SELECT user_name FROM devices WHERE id = ?", device_id)
        return {"name": dev["user_name"] if dev else "", "progress": await _load_progress(device_id)}

    @app.put("/learn/api/progress", include_in_schema=False)
    async def learn_put_progress(request: Request):
        device_id = await _device_from_cookie(request)
        if not device_id:
            return JSONResponse({"detail": "로그인이 필요해요"}, status_code=401)
        body = await request.body()
        if len(body) > MAX_BODY:
            return JSONResponse({"detail": "데이터가 너무 커요"}, status_code=413)
        try:
            payload = json.loads(body or b"{}")
        except ValueError:
            return JSONResponse({"detail": "형식이 올바르지 않아요"}, status_code=400)
        new = _clean_progress(payload.get("progress") if isinstance(payload, dict) else None)
        if not (isinstance(payload, dict) and payload.get("reset") is True):
            old = await _load_progress(device_id)
            if old:
                new = _merge(old, new)
        await DB.execute(
            upsert_sql("learning_progress", "device_id", ["data", "updated_at"]),
            device_id, json.dumps(new, ensure_ascii=False), _iso(_utcnow()),
        )
        return {"status": "ok", **_summary(new)}

    @app.get("/learn/api/tts/status", include_in_schema=False)
    async def learn_tts_status():
        """학습 페이지가 열릴 때 확인: 서버 읽어 주기를 쓸 수 있는지."""
        return {"enabled": tts_enabled(), "engine": TTS_ENGINE if tts_enabled() else "off",
                "langs": list(TTS_LANGS), "max_chars": TTS_MAX_CHARS,
                "last_ok": TTS_LAST["ok_at"], "last_error": TTS_LAST["error"], "last_error_at": TTS_LAST["error_at"]}

    @app.get("/learn/api/tts/selftest", include_in_schema=False)
    async def learn_tts_selftest(request: Request):
        """서버에서 음성을 실제로 만들어 본다(캐시를 거치지 않음). 소리 문제를 점검할 때 브라우저로 열어 보면 된다."""
        if not tts_enabled():
            return {"ok": False, "engine": "off", "error": "TTS_ENGINE=off"}
        ip = (request.headers.get("x-forwarded-for", "").split(",")[0].strip()
              or (request.client.host if request.client else ""))
        if not _tts_allowed(ip):
            return JSONResponse({"ok": False, "error": "잠시 뒤에 다시 시도해 주세요"}, status_code=429)
        started = time.time()
        try:
            data = await asyncio.wait_for(TTS_ENGINES[TTS_ENGINE]("안녕하세요. 소리 점검이에요.", "ko-KR"), timeout=25)
        except Exception as e:
            msg = f"{type(e).__name__}: {e}"[:300]
            TTS_LAST.update(error=msg, error_at=datetime.utcnow().isoformat(timespec="seconds"))
            print(f"[tts] selftest 실패: {msg}", flush=True)
            return {"ok": False, "engine": TTS_ENGINE, "error": msg, "seconds": round(time.time() - started, 2)}
        return {"ok": bool(data), "engine": TTS_ENGINE, "bytes": len(data or b""),
                "head": (data or b"")[:4].hex(), "seconds": round(time.time() - started, 2)}

    @app.api_route("/learn/api/tts", methods=["GET", "HEAD"], include_in_schema=False)
    async def learn_tts(request: Request, t: str = "", l: str = "ko-KR"):
        """읽어 주기 음성(mp3). 누구나 쓸 수 있다(학습 페이지에는 로그인이 없음). 길이·언어·횟수 제한."""
        text = " ".join(t.split())
        if not tts_enabled():
            return JSONResponse({"detail": "읽어 주기 음성이 꺼져 있어요"}, status_code=503)
        if not text or len(text) > TTS_MAX_CHARS or l not in TTS_LANGS:
            return JSONResponse({"detail": "읽을 글이 올바르지 않아요"}, status_code=400)
        ip = (request.headers.get("x-forwarded-for", "").split(",")[0].strip()
              or (request.client.host if request.client else ""))
        try:
            data = await synthesize(text, l, ip)
        except PermissionError:
            return JSONResponse({"detail": "잠시 뒤에 다시 들어 주세요"}, status_code=429)
        if not data:
            return JSONResponse({"detail": "음성을 만들지 못했어요"}, status_code=503)
        return _audio_response(request, data)

    @app.get("/learn/{name}", include_in_schema=False)
    async def learn_static(name: str):
        if name not in STATIC_FILES:
            raise HTTPException(status_code=404)
        return FileResponse(os.path.join(STATIC_DIR, name), media_type=STATIC_FILES[name])
