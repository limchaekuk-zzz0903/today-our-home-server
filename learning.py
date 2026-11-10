"""
학습 페이지(숫자섬 수학 탐험대) 연동

흐름
  1) 앱이 인증 헤더(X-Device-ID, X-Device-Secret)와 함께 POST /api/learning/session 호출
     → 5분 동안 한 번만 쓸 수 있는 입장권(ticket)이 담긴 URL을 받음
  2) 앱이 그 URL을 WebView로 염 → GET /learn/?ticket=...
     → 입장권 확인 후 로그인 쿠키(toh_learn, 30일)를 심고 /learn/ 으로 이동
  3) 학습 페이지가 /learn/api/progress 로 별·크리스탈 진도를 불러오고 저장(쿠키 인증)
  4) 앱은 GET /api/learning/family-progress 로 가족 구성원별 학습 현황을 조회

/learn 아래 경로는 기기 인증 미들웨어에서 제외되고, 이 모듈이 쿠키로 직접 인증한다.
"""
import hashlib
import json
import os
import re
import secrets
from datetime import datetime, timedelta
from typing import Optional

from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import FileResponse, HTMLResponse, JSONResponse, RedirectResponse

STATIC_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "learn_static")
STATIC_FILES = {
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
MAX_BODY = 20_000
# 학습 페이지 전체 규모 (수학·국어·영어·과학): 화 76개, 대장 문제 크리스탈 22개
TOTAL_LESSONS = 76
TOTAL_CRYSTALS = 22
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

_NEED_APP_PAGE = """<!doctype html><html lang="ko"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1"><title>숫자섬 수학 탐험대</title>
<style>body{margin:0;min-height:100vh;display:flex;align-items:center;justify-content:center;
font-family:'Apple SD Gothic Neo','Malgun Gothic',sans-serif;background:#F2F4FB;color:#1B2140;padding:24px;box-sizing:border-box}
main{max-width:360px;text-align:center;background:#fff;border:3px solid #1B2140;border-radius:12px;padding:28px;box-shadow:6px 6px 0 #1B2140}
h1{font-size:1.4rem;margin:0 0 12px}p{line-height:1.6;margin:0}</style></head>
<body><main><h1>오늘우리집 앱에서 열어 주세요</h1>
<p>숫자섬 수학 탐험대는 오늘우리집 회원 전용 학습 페이지예요.<br>앱에 로그인한 뒤 학습 메뉴에서 다시 열어 주세요.</p>
</main></body></html>"""


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


def _clean_progress(raw) -> dict:
    """앱이 보낸 진도를 검증해서 정해진 모양으로만 저장한다."""
    if not isinstance(raw, dict):
        raw = {}
    stars = {}
    for k, v in list((raw.get("stars") or {}).items())[:60]:
        if isinstance(k, str) and _LESSON_RE.match(k):
            try:
                n = int(v)
            except (TypeError, ValueError):
                continue
            if 0 < n <= 3:
                stars[k] = n
    crystals = {}
    for k, v in list((raw.get("crystals") or {}).items())[:12]:
        if isinstance(k, str) and _REGION_RE.match(k) and v is True:
            crystals[k] = True
    name = raw.get("name")
    name = name.strip()[:20] if isinstance(name, str) else ""
    return {"stars": stars, "crystals": crystals, "name": name}


def _merge(old: dict, new: dict) -> dict:
    """여러 번 저장돼도 진도가 줄지 않도록 별은 큰 값, 크리스탈은 하나라도 있으면 유지."""
    stars = dict(old.get("stars", {}))
    for k, v in new["stars"].items():
        stars[k] = max(stars.get(k, 0), v)
    crystals = {**old.get("crystals", {}), **new["crystals"]}
    return {"stars": stars, "crystals": crystals, "name": new["name"] or old.get("name", "")}


def _summary(progress: Optional[dict]) -> dict:
    p = progress or {"stars": {}, "crystals": {}}
    stars = p.get("stars", {})
    return {
        "lessons_done": sum(1 for v in stars.values() if v > 0),
        "stars_total": sum(stars.values()),
        "crystals": sorted(k for k, v in p.get("crystals", {}).items() if v),
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
        """학습 페이지를 열 1회용 URL 발급. start=g4(학년 지도), frac-1(화)처럼 시작 화면을 지정할 수 있음."""
        device_id = request.headers.get("X-Device-ID", "")
        device = await DB.fetchrow("SELECT id FROM devices WHERE id = ?", device_id)
        if not device:
            raise HTTPException(status_code=404, detail="등록되지 않은 기기예요")
        if start and not (_LESSON_RE.match(start) or start == "home" or re.match(r"^(?:(?:kor|eng|sci)-)?g[1-6]$", start)):
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
        }

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
        return {"total_lessons": TOTAL_LESSONS, "total_crystals": TOTAL_CRYSTALS, "members": result}

    # ── 학습 페이지 (쿠키 인증) ──────────────────────────────────────────────

    @app.get("/learn", include_in_schema=False)
    async def learn_redirect(request: Request):
        q = f"?{request.url.query}" if request.url.query else ""
        return RedirectResponse(f"/learn/{q}", status_code=307)

    @app.get("/learn/", include_in_schema=False)
    async def learn_page(request: Request, ticket: Optional[str] = None):
        if ticket:
            row = await DB.fetchrow(
                "SELECT device_id, start, expires_at, used FROM learning_tickets WHERE ticket_hash = ?", _hash(ticket)
            )
            if not row or row["used"] or row["expires_at"] < _iso(_utcnow()):
                return HTMLResponse(_NEED_APP_PAGE, status_code=403)
            await DB.execute("UPDATE learning_tickets SET used = 1 WHERE ticket_hash = ?", _hash(ticket))
            token = secrets.token_urlsafe(32)
            now = _utcnow()
            await DB.execute(
                "INSERT INTO learning_sessions (token_hash, device_id, created_at, expires_at) VALUES (?, ?, ?, ?)",
                _hash(token), row["device_id"], _iso(now), _iso(now + SESSION_TTL),
            )
            target = "/learn/" + (f"#{row['start']}" if row.get("start") else "")
            resp = RedirectResponse(target, status_code=303)
            resp.set_cookie(
                COOKIE, token, max_age=int(SESSION_TTL.total_seconds()), path=COOKIE_PATH,
                httponly=True, samesite="lax", secure=_is_https(request),
            )
            return resp

        if not await _device_from_cookie(request):
            return HTMLResponse(_NEED_APP_PAGE, status_code=403)
        return FileResponse(
            os.path.join(STATIC_DIR, "index.html"),
            media_type="text/html; charset=utf-8",
            headers={"Cache-Control": "no-cache", "X-Frame-Options": "SAMEORIGIN"},
        )

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

    @app.get("/learn/{name}", include_in_schema=False)
    async def learn_static(name: str):
        if name not in STATIC_FILES:
            raise HTTPException(status_code=404)
        return FileResponse(os.path.join(STATIC_DIR, name), media_type=STATIC_FILES[name])
