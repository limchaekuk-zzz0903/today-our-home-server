"""
오늘우리집 서버 v4.0
- asyncpg (PostgreSQL) 또는 aiosqlite (로컬) 사용
- SQLAlchemy 의존성 제거
"""
import os
import ssl
import uuid
import random
import string
import hashlib
from datetime import datetime, timedelta
from typing import List, Optional

import asyncpg
import uvicorn
from fastapi import FastAPI, HTTPException, Request, Response
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from pydantic import BaseModel

# ── DB 설정 ───────────────────────────────────────────────────────────────────

_DB_URL = os.environ.get("DATABASE_URL", "")
if _DB_URL.startswith("postgres://"):
    _DB_URL = _DB_URL.replace("postgres://", "postgresql://", 1)

_USE_PG = bool(_DB_URL and "postgresql" in _DB_URL)

# SQLite fallback (로컬 개발용)
if not _USE_PG:
    import sqlite3, threading
    _sqlite_path = os.path.join(os.path.dirname(__file__), "today_our_home_server.db")
    _sqlite_lock = threading.Lock()

# ── DB 연결 헬퍼 ──────────────────────────────────────────────────────────────

_pg_pool: Optional[asyncpg.Pool] = None


async def get_pg_pool() -> asyncpg.Pool:
    global _pg_pool
    if _pg_pool is None:
        # Railway PostgreSQL requires SSL; disable cert verification for internal connections
        ssl_ctx = ssl.create_default_context()
        ssl_ctx.check_hostname = False
        ssl_ctx.verify_mode = ssl.CERT_NONE
        # Strip sslmode param from URL — asyncpg uses ssl= kwarg instead
        url = _DB_URL.split("?")[0]
        _pg_pool = await asyncpg.create_pool(url, min_size=1, max_size=10, ssl=ssl_ctx)
    return _pg_pool


class DB:
    """PostgreSQL(asyncpg) 또는 SQLite(sqlite3) 추상화 레이어"""

    @staticmethod
    async def execute(sql: str, *args):
        if _USE_PG:
            pool = await get_pg_pool()
            async with pool.acquire() as conn:
                await conn.execute(_pg_sql(sql), *args)
        else:
            _sqlite_run(sql, args)

    @staticmethod
    async def fetchrow(sql: str, *args) -> Optional[dict]:
        if _USE_PG:
            pool = await get_pg_pool()
            async with pool.acquire() as conn:
                row = await conn.fetchrow(_pg_sql(sql), *args)
                return dict(row) if row else None
        else:
            return _sqlite_fetchrow(sql, args)

    @staticmethod
    async def fetch(sql: str, *args) -> List[dict]:
        if _USE_PG:
            pool = await get_pg_pool()
            async with pool.acquire() as conn:
                rows = await conn.fetch(_pg_sql(sql), *args)
                return [dict(r) for r in rows]
        else:
            return _sqlite_fetch(sql, args)

    @staticmethod
    async def fetchval(sql: str, *args):
        if _USE_PG:
            pool = await get_pg_pool()
            async with pool.acquire() as conn:
                return await conn.fetchval(_pg_sql(sql), *args)
        else:
            row = _sqlite_fetchrow(sql, args)
            return list(row.values())[0] if row else None


def _pg_sql(sql: str) -> str:
    """? 를 $1, $2, ... 로 변환 (asyncpg 파라미터 스타일)"""
    result = []
    i = 1
    for ch in sql:
        if ch == '?':
            result.append(f'${i}')
            i += 1
        else:
            result.append(ch)
    return ''.join(result)


def _sqlite_run(sql: str, args=()):
    with _sqlite_lock:
        con = sqlite3.connect(_sqlite_path)
        con.row_factory = sqlite3.Row
        try:
            con.execute(sql, args)
            con.commit()
        finally:
            con.close()


def _sqlite_fetchrow(sql: str, args=()) -> Optional[dict]:
    with _sqlite_lock:
        con = sqlite3.connect(_sqlite_path)
        con.row_factory = sqlite3.Row
        try:
            row = con.execute(sql, args).fetchone()
            return dict(row) if row else None
        finally:
            con.close()


def _sqlite_fetch(sql: str, args=()) -> List[dict]:
    with _sqlite_lock:
        con = sqlite3.connect(_sqlite_path)
        con.row_factory = sqlite3.Row
        try:
            rows = con.execute(sql, args).fetchall()
            return [dict(r) for r in rows]
        finally:
            con.close()


# ── DB 초기화 ─────────────────────────────────────────────────────────────────

_INIT_STMTS = [
    """CREATE TABLE IF NOT EXISTS users (
        id               TEXT PRIMARY KEY,
        provider  TEXT,
        social_id        TEXT,
        name             TEXT,
        email            TEXT,
        password_hash    TEXT,
        profile_image_url TEXT,
        created_at       TEXT NOT NULL
    )""",
    """CREATE TABLE IF NOT EXISTS device_secrets (
        device_id   TEXT PRIMARY KEY,
        secret      TEXT NOT NULL,
        user_id     TEXT,
        created_at  TEXT NOT NULL
    )""",
    """CREATE TABLE IF NOT EXISTS devices (
        id          TEXT PRIMARY KEY,
        user_name   TEXT NOT NULL,
        family_id   TEXT,
        created_at  TEXT NOT NULL
    )""",
    """CREATE TABLE IF NOT EXISTS families (
        id              TEXT PRIMARY KEY,
        name            TEXT NOT NULL,
        created_by  TEXT NOT NULL,
        created_at      TEXT NOT NULL
    )""",
    """CREATE TABLE IF NOT EXISTS invite_codes (
        code        TEXT PRIMARY KEY,
        family_id   TEXT NOT NULL,
        created_by  TEXT NOT NULL,
        created_at  TEXT NOT NULL,
        expires_at  TEXT NOT NULL,
        used     INTEGER DEFAULT 0
    )""",
    """CREATE TABLE IF NOT EXISTS join_requests (
        id                   TEXT PRIMARY KEY,
        family_id            TEXT NOT NULL,
        device_id  TEXT NOT NULL,
        device_name       TEXT NOT NULL,
        status               TEXT DEFAULT 'pending',
        created_at           TEXT NOT NULL
    )""",
    """CREATE TABLE IF NOT EXISTS shared_events (
        id           TEXT PRIMARY KEY,
        family_id    TEXT NOT NULL,
        created_by   TEXT NOT NULL,
        title        TEXT NOT NULL,
        description  TEXT,
        start_time   TEXT NOT NULL,
        end_time     TEXT,
        category     INTEGER NOT NULL DEFAULT 0,
        preparations TEXT DEFAULT '[]',
        needs_reply  INTEGER DEFAULT 0,
        is_completed INTEGER DEFAULT 0,
        updated_at   TEXT NOT NULL
    )""",
    """CREATE TABLE IF NOT EXISTS device_phones (
        device_id    TEXT PRIMARY KEY,
        phone_number TEXT NOT NULL UNIQUE,
        updated_at   TEXT NOT NULL
    )""",
    """CREATE TABLE IF NOT EXISTS contact_invites (
        id             TEXT PRIMARY KEY,
        family_id      TEXT NOT NULL,
        from_device_id TEXT NOT NULL,
        to_device_id   TEXT NOT NULL,
        status         TEXT DEFAULT 'pending',
        created_at     TEXT NOT NULL
    )""",
]


async def init_db():
    for stmt in _INIT_STMTS:
        try:
            await DB.execute(stmt)
        except Exception as e:
            print(f"init_db warning: {e}")

    # ALTER TABLE 마이그레이션 (기존 컬럼 없는 경우 추가)
    for alter in [
        # users 컬럼 추가 (기존 DB 호환)
        "ALTER TABLE users ADD COLUMN email TEXT",
        "ALTER TABLE users ADD COLUMN password_hash TEXT",
        "ALTER TABLE users ADD COLUMN profile_image_url TEXT",
        # devices 컬럼 추가 (구 서버 스키마엔 family_id/created_at 없음)
        "ALTER TABLE devices ADD COLUMN family_id TEXT",
        "ALTER TABLE devices ADD COLUMN created_at TEXT",
    ]:
        try:
            await DB.execute(alter)
        except Exception:
            pass  # 이미 컬럼 존재하면 무시


# ── FastAPI 앱 ─────────────────────────────────────────────────────────────────

app = FastAPI(title="오늘우리집 서버", version="4.0.0")
app.add_middleware(
    CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"]
)


@app.on_event("startup")
async def startup():
    await init_db()


# ── 인증 미들웨어 ─────────────────────────────────────────────────────────────

_NO_AUTH_PATHS = {"/api/ping", "/api/debug/db", "/api/debug/register-test", "/api/debug/user", "/api/auth/social", "/api/auth/email/register", "/api/auth/email/login"}


@app.middleware("http")
async def device_auth_middleware(request: Request, call_next):
    path = request.url.path
    if path in _NO_AUTH_PATHS or path.startswith("/docs") or path.startswith("/openapi"):
        return await call_next(request)

    device_id = request.headers.get("X-Device-ID", "")
    device_secret = request.headers.get("X-Device-Secret", "")

    if not device_id or not device_secret:
        return JSONResponse(
            {"detail": "인증 헤더가 없어요 (X-Device-ID, X-Device-Secret)"},
            status_code=401,
        )

    row = await DB.fetchrow(
        "SELECT secret FROM device_secrets WHERE device_id = ?", device_id
    )
    if not row or row["secret"] != device_secret:
        return JSONResponse(
            {"detail": "인증에 실패했어요. 다시 로그인해 주세요."},
            status_code=401,
        )

    return await call_next(request)

# ── Pydantic 모델 ─────────────────────────────────────────────────────────────

class SocialAuthReq(BaseModel):
    provider: str
    social_id: str
    name: str
    device_id: str
    email: Optional[str] = None
    profile_image_url: Optional[str] = None

class EmailRegisterReq(BaseModel):
    email: str
    password: str
    name: str
    device_id: str

class EmailLoginReq(BaseModel):
    email: str
    password: str
    device_id: str

class DeviceRegisterReq(BaseModel):
    id: str
    user_name: str

class PhoneRegisterReq(BaseModel):
    device_id: str
    phone_number: str

class PhoneLookupReq(BaseModel):
    phone_numbers: List[str]

class FamilyCreateReq(BaseModel):
    device_id: str
    family_name: str

class JoinReq(BaseModel):
    code: str
    device_id: str
    device_name: str

class ConfirmReq(BaseModel):
    request_id: str
    device_id: str
    action: str

class ContactInviteReq(BaseModel):
    from_device_id: str
    to_device_id: str

class InviteRespondReq(BaseModel):
    device_id: str
    action: str

class UpdateNameReq(BaseModel):
    device_id: str
    name: str

class SharedEventReq(BaseModel):
    id: str
    family_id: str
    created_by: str
    title: str
    description: Optional[str] = None
    start_time: str
    end_time: Optional[str] = None
    category: int = 0
    preparations: str = "[]"
    needs_reply: int = 0
    is_completed: int = 0
    updated_at: str

# ── 헬퍼 ─────────────────────────────────────────────────────────────────────

def _now() -> str:
    return datetime.now().isoformat()


def _hash_pw(pw: str) -> str:
    return hashlib.sha256(pw.encode()).hexdigest()


async def _get_device(device_id: str) -> dict:
    row = await DB.fetchrow("SELECT * FROM devices WHERE id = ?", device_id)
    if not row:
        raise HTTPException(status_code=404, detail="등록되지 않은 기기예요")
    return row


async def _require_family(device: dict) -> str:
    if not device.get("family_id"):
        raise HTTPException(status_code=400, detail="가족 그룹에 속해 있지 않아요")
    return device["family_id"]


async def _upsert_device_secret(device_id: str, secret: str, user_id: str):
    if _USE_PG:
        await DB.execute("""
            INSERT INTO device_secrets (device_id, secret, user_id, created_at)
            VALUES ($1, $2, $3, NOW())
            ON CONFLICT (device_id) DO UPDATE SET secret = EXCLUDED.secret, user_id = EXCLUDED.user_id
        """, device_id, secret, user_id)
    else:
        await DB.execute("""
            INSERT OR REPLACE INTO device_secrets (device_id, secret, user_id, created_at)
            VALUES (?, ?, ?, ?)
        """, device_id, secret, user_id, _now())


async def _upsert_device(device_id: str, name: str):
    if _USE_PG:
        await DB.execute("""
            INSERT INTO devices (id, user_name, created_at)
            VALUES ($1, $2, NOW())
            ON CONFLICT (id) DO UPDATE SET user_name = EXCLUDED.user_name
        """, device_id, name)
    else:
        await DB.execute("""
            INSERT INTO devices (id, user_name, created_at)
            VALUES (?, ?, ?)
            ON CONFLICT(id) DO UPDATE SET user_name = excluded.user_name
        """, device_id, name, _now())


async def _get_family_members(family_id: str):
    family = await DB.fetchrow("SELECT * FROM families WHERE id = ?", family_id)
    if not family:
        return None, []
    members_raw = await DB.fetch(
        "SELECT id, user_name FROM devices WHERE family_id = ?", family_id
    )
    members = [
        {
            "device_id": m["id"],
            "user_name": m["user_name"],
            "is_creator": m["id"] == family["created_by"],
        }
        for m in members_raw
    ]
    return family, members

# ── 엔드포인트 ────────────────────────────────────────────────────────────────

@app.get("/api/ping")
async def ping():
    return {"status": "ok", "message": "오늘우리집 서버 정상 동작 중", "build": "v4-fix-15"}


@app.get("/api/debug/db", include_in_schema=False)
async def debug_db():
    """임시 디버그: DB 연결 상태 확인 및 테이블 컬럼 조회"""
    try:
        row = await DB.fetchrow("SELECT 1 AS val")
        cols = await DB.fetch(
            "SELECT column_name, data_type FROM information_schema.columns WHERE table_name='users' ORDER BY ordinal_position"
        ) if _USE_PG else []
        ts_cols = await DB.fetch("""
            SELECT table_name, column_name, data_type
            FROM information_schema.columns
            WHERE data_type LIKE '%timestamp%'
            ORDER BY table_name, ordinal_position
        """) if _USE_PG else []
        return {
            "db": "ok", "use_pg": _USE_PG,
            "users_cols": {r["column_name"]: r["data_type"] for r in cols},
            "timestamp_columns": [f"{r['table_name']}.{r['column_name']}" for r in ts_cols],
            "devices_cols": {r["column_name"]: r["data_type"] for r in (await DB.fetch(
                "SELECT column_name, data_type FROM information_schema.columns WHERE table_name='devices' ORDER BY ordinal_position"
            ) if _USE_PG else [])},
            "families_cols": {r["column_name"]: r["data_type"] for r in (await DB.fetch(
                "SELECT column_name, data_type FROM information_schema.columns WHERE table_name='families' ORDER BY ordinal_position"
            ) if _USE_PG else [])},
            "invite_codes_cols": {r["column_name"]: r["data_type"] for r in (await DB.fetch(
                "SELECT column_name, data_type FROM information_schema.columns WHERE table_name='invite_codes' ORDER BY ordinal_position"
            ) if _USE_PG else [])},
            "join_requests_cols": {r["column_name"]: r["data_type"] for r in (await DB.fetch(
                "SELECT column_name, data_type FROM information_schema.columns WHERE table_name='join_requests' ORDER BY ordinal_position"
            ) if _USE_PG else [])},
        }
    except Exception as e:
        return {"db": "error", "error": str(e), "use_pg": _USE_PG}


@app.get("/api/debug/user", include_in_schema=False)
async def debug_user(email: str, pw: Optional[str] = None):
    """임시 디버그: 특정 이메일 사용자 조회"""
    rows = await DB.fetch("SELECT id, email, provider, password_hash FROM users WHERE email = ?", email)
    result = {"count": len(rows), "rows": [dict(r) for r in rows]}
    if pw and rows:
        hashed = _hash_pw(pw)
        result["input_hash"] = hashed
        result["db_hash"] = rows[0].get("password_hash")
        result["hash_match"] = hashed == rows[0].get("password_hash")
        # direct query test
        match = await DB.fetchrow(
            "SELECT id FROM users WHERE email = ? AND password_hash = ? AND (provider IS NULL OR provider = 'email')",
            email, hashed
        )
        result["direct_login_result"] = dict(match) if match else None
    return result


@app.post("/api/debug/register-test", include_in_schema=False)
async def debug_register_test():
    """임시 디버그: 이메일 회원가입 과정별 에러 확인"""
    errors = []
    try:
        existing = await DB.fetchrow(
            "SELECT id FROM users WHERE email = ? AND (provider IS NULL OR provider = 'email')", "debug@test.com"
        )
        errors.append(f"step1_ok: {existing}")
    except Exception as e:
        errors.append(f"step1_fail: {e}")
        return {"errors": errors}
    try:
        uid = str(uuid.uuid4())
        if _USE_PG:
            await DB.execute(
                "INSERT INTO users (id, name, email, password_hash, created_at) VALUES ($1, $2, $3, $4, NOW())",
                uid, "디버그", "debug@test.com", "hash",
            )
        else:
            await DB.execute(
                "INSERT INTO users (id, name, email, password_hash, created_at) VALUES (?, ?, ?, ?, ?)",
                uid, "디버그", "debug@test.com", "hash", _now(),
            )
        errors.append(f"step2_ok: inserted {uid}")
        await DB.execute("DELETE FROM users WHERE id = ?", uid)
    except Exception as e:
        errors.append(f"step2_fail: {e}")
        return {"errors": errors}
    try:
        await _upsert_device_secret("debug-dev", "debug-sec", "debug-uid")
        errors.append("step3_device_secret: ok")
        await DB.execute("DELETE FROM device_secrets WHERE device_id = ?", "debug-dev")
    except Exception as e:
        errors.append(f"step3_device_secret_fail: {e}")
        return {"errors": errors}
    try:
        await _upsert_device("debug-dev", "디버그디바이스")
        errors.append("step4_device: ok")
        await DB.execute("DELETE FROM devices WHERE id = ?", "debug-dev")
    except Exception as e:
        errors.append(f"step4_device_fail: {e}")
    return {"errors": errors}


@app.post("/api/auth/social")
async def social_auth(data: SocialAuthReq):
    """소셜 로그인 처리: 사용자 upsert + 기기 시크릿 발급"""
    user = await DB.fetchrow(
        "SELECT * FROM users WHERE provider = ? AND social_id = ?",
        data.provider, data.social_id,
    )

    if user is None:
        user_id = str(uuid.uuid4())
        if _USE_PG:
            await DB.execute(
                "INSERT INTO users (id, provider, social_id, name, email, profile_image_url, created_at)"
                " VALUES ($1, $2, $3, $4, $5, $6, NOW())",
                user_id, data.provider, data.social_id, data.name,
                data.email, data.profile_image_url,
            )
        else:
            await DB.execute(
                "INSERT INTO users (id, provider, social_id, name, email, profile_image_url, created_at)"
                " VALUES (?, ?, ?, ?, ?, ?, ?)",
                user_id, data.provider, data.social_id, data.name,
                data.email, data.profile_image_url, _now(),
            )
    else:
        user_id = user["id"]
        await DB.execute(
            "UPDATE users SET name = ?, email = COALESCE(?, email), profile_image_url = COALESCE(?, profile_image_url) WHERE id = ?",
            data.name, data.email, data.profile_image_url, user_id,
        )

    device_secret = str(uuid.uuid4())
    await _upsert_device_secret(data.device_id, device_secret, user_id)
    await _upsert_device(data.device_id, data.name)

    return {"device_secret": device_secret, "user_id": user_id}


@app.post("/api/auth/email/register")
async def email_register(data: EmailRegisterReq):
    """이메일 회원가입"""
    existing = await DB.fetchrow(
        "SELECT id FROM users WHERE email = ? AND (provider IS NULL OR provider = 'email')", data.email
    )
    if existing:
        raise HTTPException(status_code=400, detail="이미 사용 중인 이메일이에요")

    user_id = str(uuid.uuid4())
    if _USE_PG:
        await DB.execute(
            "INSERT INTO users (id, name, email, password_hash, provider, created_at) VALUES ($1, $2, $3, $4, 'email', NOW())",
            user_id, data.name, data.email, _hash_pw(data.password),
        )
    else:
        await DB.execute(
            "INSERT INTO users (id, name, email, password_hash, provider, created_at) VALUES (?, ?, ?, ?, 'email', ?)",
            user_id, data.name, data.email, _hash_pw(data.password), _now(),
        )

    device_secret = str(uuid.uuid4())
    await _upsert_device_secret(data.device_id, device_secret, user_id)
    await _upsert_device(data.device_id, data.name)

    return {"user_id": user_id, "token": device_secret, "name": data.name}


@app.post("/api/auth/email/login")
async def email_login(data: EmailLoginReq):
    """이메일 로그인"""
    user = await DB.fetchrow(
        "SELECT * FROM users WHERE email = ? AND password_hash = ? AND (provider IS NULL OR provider = 'email')",
        data.email, _hash_pw(data.password),
    )
    if not user:
        raise HTTPException(status_code=401, detail="이메일 또는 비밀번호가 일치하지 않아요")

    device_secret = str(uuid.uuid4())
    await _upsert_device_secret(data.device_id, device_secret, user["id"])
    await _upsert_device(data.device_id, user["name"])

    return {
        "user_id": user["id"],
        "token": device_secret,
        "name": user["name"],
        "profile_image_url": user.get("profile_image_url"),
    }


@app.get("/api/auth/restore")
async def auth_restore(user_id: str, device_id: Optional[str] = None):
    """재로그인 없이 사용자 정보 복원"""
    user = await DB.fetchrow("SELECT * FROM users WHERE id = ?", user_id)
    if not user:
        raise HTTPException(status_code=404, detail="사용자를 찾을 수 없어요")

    device = None
    if device_id:
        device = await DB.fetchrow("SELECT * FROM devices WHERE id = ?", device_id)

    family_id = device["family_id"] if device else None
    family_name = None
    if family_id:
        family = await DB.fetchrow("SELECT name FROM families WHERE id = ?", family_id)
        family_name = family["name"] if family else None

    return {
        "user_id": user["id"],
        "name": user["name"],
        "email": user.get("email"),
        "provider": user.get("provider") or "email",
        "profile_image_url": user.get("profile_image_url"),
        "family_id": family_id,
        "family_name": family_name,
    }


@app.post("/api/devices/register")
async def register_device(data: DeviceRegisterReq):
    await _upsert_device(data.id, data.user_name)
    device = await DB.fetchrow("SELECT family_id FROM devices WHERE id = ?", data.id)
    family_id = device["family_id"] if device else None
    family_name = None
    if family_id:
        family = await DB.fetchrow("SELECT name FROM families WHERE id = ?", family_id)
        family_name = family["name"] if family else None
    return {"status": "ok", "device_id": data.id, "family_id": family_id, "family_name": family_name}


@app.post("/api/devices/phone")
async def register_phone(data: PhoneRegisterReq):
    await _get_device(data.device_id)
    if _USE_PG:
        await DB.execute("""
            INSERT INTO device_phones (device_id, phone_number, updated_at)
            VALUES (?, ?, ?)
            ON CONFLICT (device_id) DO UPDATE SET phone_number = EXCLUDED.phone_number, updated_at = EXCLUDED.updated_at
        """, data.device_id, data.phone_number, _now())
    else:
        await DB.execute("""
            INSERT INTO device_phones (device_id, phone_number, updated_at)
            VALUES (?, ?, ?)
            ON CONFLICT(device_id) DO UPDATE SET phone_number = excluded.phone_number, updated_at = excluded.updated_at
        """, data.device_id, data.phone_number, _now())
    return {"status": "ok"}


@app.post("/api/devices/lookup")
async def lookup_contacts(data: PhoneLookupReq):
    if not data.phone_numbers:
        return {"found": []}
    placeholders = ",".join(["?" for _ in data.phone_numbers])
    rows = await DB.fetch(f"""
        SELECT dp.phone_number, dp.device_id, d.user_name
        FROM device_phones dp
        JOIN devices d ON d.id = dp.device_id
        WHERE dp.phone_number IN ({placeholders})
    """, *data.phone_numbers)
    return {"found": [
        {"phone": r["phone_number"], "device_id": r["device_id"], "user_name": r["user_name"]}
        for r in rows
    ]}


@app.post("/api/family/create")
async def create_family(data: FamilyCreateReq):
    device = await _get_device(data.device_id)
    if device["family_id"]:
        family = await DB.fetchrow("SELECT * FROM families WHERE id = ?", device["family_id"])
        return {"family_id": family["id"], "family_name": family["name"]}

    family_id = str(uuid.uuid4())
    if _USE_PG:
        await DB.execute(
            "INSERT INTO families (id, name, created_by, created_at) VALUES ($1, $2, $3, NOW())",
            family_id, data.family_name, data.device_id,
        )
    else:
        await DB.execute(
            "INSERT INTO families (id, name, created_by, created_at) VALUES (?, ?, ?, ?)",
            family_id, data.family_name, data.device_id, _now(),
        )
    await DB.execute("UPDATE devices SET family_id = ? WHERE id = ?", family_id, data.device_id)
    return {"family_id": family_id, "family_name": data.family_name}


@app.get("/api/family/my-families")
async def get_my_families(device_id: str):
    device = await _get_device(device_id)
    if not device["family_id"]:
        return {"families": []}
    family = await DB.fetchrow("SELECT * FROM families WHERE id = ?", device["family_id"])
    if not family:
        return {"families": []}
    member_count = await DB.fetchval(
        "SELECT COUNT(*) FROM devices WHERE family_id = ?", device["family_id"]
    )
    return {"families": [{
        "family_id": family["id"],
        "family_name": family["name"],
        "created_by": family["created_by"],
        "member_count": member_count or 0,
    }]}


@app.post("/api/family/invite-code")
async def create_invite_code(device_id: str):
    device = await _get_device(device_id)
    family_id = await _require_family(device)
    code = "".join(random.choices(string.digits, k=6))
    expires_dt = datetime.now() + timedelta(hours=24)
    if _USE_PG:
        await DB.execute("""
            INSERT INTO invite_codes (code, family_id, created_by, expires_at, used)
            VALUES ($1, $2, $3, NOW() + INTERVAL '24 hours', FALSE)
            ON CONFLICT (code) DO UPDATE SET
                family_id = EXCLUDED.family_id, created_by = EXCLUDED.created_by,
                expires_at = EXCLUDED.expires_at, used = FALSE
        """, code, family_id, device_id)
    else:
        await DB.execute("""
            INSERT OR REPLACE INTO invite_codes (code, family_id, created_by, created_at, expires_at, used)
            VALUES (?, ?, ?, ?, ?, 0)
        """, code, family_id, device_id, _now(), expires_dt.isoformat())
    return {"code": code, "expires_at": expires_dt.isoformat()}


@app.post("/api/family/join")
async def join_family(data: JoinReq):
    invite = await DB.fetchrow(
        "SELECT * FROM invite_codes WHERE code = ? AND used = FALSE AND expires_at > NOW()" if _USE_PG
        else "SELECT * FROM invite_codes WHERE code = ? AND used = 0 AND expires_at > ?",
        *([data.code] if _USE_PG else [data.code, _now()]),
    )
    if not invite:
        raise HTTPException(status_code=404, detail="유효하지 않은 초대 코드예요")

    device = await DB.fetchrow("SELECT * FROM devices WHERE id = ?", data.device_id)
    if device and device["family_id"] == invite["family_id"]:
        raise HTTPException(status_code=400, detail="이미 이 가족 그룹에 속해 있어요")

    if not device:
        if _USE_PG:
            await DB.execute(
                "INSERT INTO devices (id, user_name, created_at) VALUES ($1, $2, NOW())",
                data.device_id, data.device_name,
            )
        else:
            await DB.execute(
                "INSERT INTO devices (id, user_name, created_at) VALUES (?, ?, ?)",
                data.device_id, data.device_name, _now(),
            )
    else:
        await DB.execute("UPDATE devices SET user_name = ? WHERE id = ?", data.device_name, data.device_id)

    request_id = str(uuid.uuid4())
    if _USE_PG:
        await DB.execute(
            "INSERT INTO join_requests (id, family_id, device_id, device_name, status, created_at)"
            " VALUES ($1, $2, $3, $4, 'pending', NOW())",
            request_id, invite["family_id"], data.device_id, data.device_name,
        )
    else:
        await DB.execute(
            "INSERT INTO join_requests (id, family_id, device_id, device_name, status, created_at)"
            " VALUES (?, ?, ?, ?, 'pending', ?)",
            request_id, invite["family_id"], data.device_id, data.device_name, _now(),
        )
    return {"request_id": request_id, "status": "pending"}


@app.post("/api/family/confirm")
async def confirm_join(data: ConfirmReq):
    request = await DB.fetchrow("SELECT * FROM join_requests WHERE id = ?", data.request_id)
    if not request:
        raise HTTPException(status_code=404, detail="요청을 찾을 수 없어요")
    if request["status"] != "pending":
        raise HTTPException(status_code=400, detail="이미 처리된 요청이에요")

    authorizer = await DB.fetchrow(
        "SELECT * FROM devices WHERE id = ? AND family_id = ?",
        data.device_id, request["family_id"],
    )
    if not authorizer:
        raise HTTPException(status_code=403, detail="승인 권한이 없어요")

    if data.action == "confirm":
        await DB.execute("UPDATE join_requests SET status = 'confirmed' WHERE id = ?", data.request_id)
        await DB.execute(
            "UPDATE devices SET family_id = ? WHERE id = ?",
            request["family_id"], request["device_id"],
        )
    else:
        await DB.execute("UPDATE join_requests SET status = 'rejected' WHERE id = ?", data.request_id)
    return {"status": f"{data.action}ed"}


@app.get("/api/family/pending-requests")
async def get_pending_requests(device_id: str, family_id: Optional[str] = None):
    device = await _get_device(device_id)
    fid = family_id or device["family_id"]
    if not fid:
        return {"requests": []}
    rows = await DB.fetch(
        "SELECT id, device_id, device_name AS requester_name, status"
        " FROM join_requests WHERE family_id = ? AND status = 'pending'",
        fid,
    )
    return {"requests": rows}


@app.get("/api/family/join-status")
async def get_join_status(request_id: str):
    row = await DB.fetchrow(
        "SELECT status, family_id FROM join_requests WHERE id = ?", request_id
    )
    if not row:
        raise HTTPException(status_code=404, detail="요청을 찾을 수 없어요")
    return {"status": row["status"], "family_id": row["family_id"]}


@app.get("/api/family/members")
async def get_family_members(device_id: str):
    device = await _get_device(device_id)
    if not device["family_id"]:
        return {"family_id": None, "family_name": None, "created_by": None, "members": []}
    family, members = await _get_family_members(device["family_id"])
    return {
        "family_id": device["family_id"],
        "family_name": family["name"] if family else None,
        "created_by": family["created_by"] if family else None,
        "members": members,
    }


@app.get("/api/family/members-v2")
async def get_family_members_v2(device_id: str, family_id: Optional[str] = None):
    device = await _get_device(device_id)
    fid = family_id or device["family_id"]
    if not fid:
        return {"family_id": None, "family_name": None, "created_by": None, "members": []}
    family, members = await _get_family_members(fid)
    return {
        "family_id": fid,
        "family_name": family["name"] if family else None,
        "created_by": family["created_by"] if family else None,
        "members": members,
    }


@app.post("/api/family/contact-invite")
async def send_contact_invite(data: ContactInviteReq):
    sender = await _get_device(data.from_device_id)
    family_id = await _require_family(sender)

    recipient = await DB.fetchrow("SELECT * FROM devices WHERE id = ?", data.to_device_id)
    if not recipient:
        raise HTTPException(status_code=404, detail="해당 사용자를 찾을 수 없어요")
    if recipient["family_id"] == family_id:
        raise HTTPException(status_code=400, detail="이미 같은 가족이에요")

    existing = await DB.fetchrow("""
        SELECT id FROM contact_invites
        WHERE family_id = ? AND from_device_id = ? AND to_device_id = ? AND status = 'pending'
    """, family_id, data.from_device_id, data.to_device_id)
    if existing:
        return {"status": "ok", "invite_id": existing["id"]}

    invite_id = str(uuid.uuid4())
    await DB.execute(
        "INSERT INTO contact_invites (id, family_id, from_device_id, to_device_id, created_at)"
        " VALUES (?, ?, ?, ?, ?)",
        invite_id, family_id, data.from_device_id, data.to_device_id, _now(),
    )
    return {"status": "ok", "invite_id": invite_id}


@app.get("/api/family/received-invitations")
async def get_received_invitations(device_id: str):
    rows = await DB.fetch("""
        SELECT ci.id, ci.created_at,
               d.user_name AS from_name,
               f.name      AS family_name
        FROM contact_invites ci
        JOIN devices  d ON d.id = ci.from_device_id
        JOIN families f ON f.id = ci.family_id
        WHERE ci.to_device_id = ? AND ci.status = 'pending'
        ORDER BY ci.created_at DESC
    """, device_id)
    return {"invitations": [
        {"id": r["id"], "from_name": r["from_name"],
         "family_name": r["family_name"], "created_at": r["created_at"]}
        for r in rows
    ]}


@app.post("/api/family/invitations/{invite_id}/respond")
async def respond_to_invitation(invite_id: str, data: InviteRespondReq):
    invite = await DB.fetchrow("SELECT * FROM contact_invites WHERE id = ?", invite_id)
    if not invite:
        raise HTTPException(status_code=404, detail="초대를 찾을 수 없어요")
    if invite["status"] != "pending":
        raise HTTPException(status_code=400, detail="이미 처리된 초대예요")
    if invite["to_device_id"] != data.device_id:
        raise HTTPException(status_code=403, detail="권한이 없어요")

    if data.action == "accept":
        await DB.execute("UPDATE contact_invites SET status = 'accepted' WHERE id = ?", invite_id)
        await DB.execute("UPDATE devices SET family_id = ? WHERE id = ?", invite["family_id"], data.device_id)
        family = await DB.fetchrow("SELECT * FROM families WHERE id = ?", invite["family_id"])
        return {
            "status": "accepted",
            "family_id": invite["family_id"],
            "family_name": family["name"] if family else None,
        }
    else:
        await DB.execute("UPDATE contact_invites SET status = 'rejected' WHERE id = ?", invite_id)
        return {"status": "rejected"}


@app.delete("/api/family/group")
async def delete_family(device_id: str):
    device = await _get_device(device_id)
    family_id = await _require_family(device)
    family = await DB.fetchrow("SELECT * FROM families WHERE id = ?", family_id)
    if not family or family["created_by"] != device_id:
        raise HTTPException(status_code=403, detail="방장만 가족 그룹을 삭제할 수 있어요")

    await DB.execute("UPDATE devices SET family_id = NULL WHERE family_id = ?", family_id)
    await DB.execute("DELETE FROM shared_events WHERE family_id = ?", family_id)
    await DB.execute("DELETE FROM invite_codes WHERE family_id = ?", family_id)
    await DB.execute("DELETE FROM join_requests WHERE family_id = ?", family_id)
    await DB.execute("DELETE FROM contact_invites WHERE family_id = ?", family_id)
    await DB.execute("DELETE FROM families WHERE id = ?", family_id)
    return {"status": "ok"}


@app.patch("/api/family/members/{member_device_id}/name")
async def update_member_name(member_device_id: str, data: UpdateNameReq):
    requester = await _get_device(data.device_id)
    target = await _get_device(member_device_id)
    if requester["family_id"] != target["family_id"]:
        raise HTTPException(status_code=403, detail="같은 가족이 아니에요")
    family = await DB.fetchrow("SELECT * FROM families WHERE id = ?", requester["family_id"])
    is_admin = family and family["created_by"] == data.device_id
    if data.device_id != member_device_id and not is_admin:
        raise HTTPException(status_code=403, detail="이름 수정 권한이 없어요")
    await DB.execute("UPDATE devices SET user_name = ? WHERE id = ?", data.name, member_device_id)
    return {"status": "ok"}


@app.delete("/api/family/members/{member_device_id}")
async def remove_member(member_device_id: str, device_id: str):
    requester = await _get_device(device_id)
    target = await _get_device(member_device_id)
    if requester["family_id"] != target["family_id"]:
        raise HTTPException(status_code=403, detail="같은 가족이 아니에요")
    family = await DB.fetchrow("SELECT * FROM families WHERE id = ?", requester["family_id"])
    is_admin = family and family["created_by"] == device_id
    if device_id != member_device_id and not is_admin:
        raise HTTPException(status_code=403, detail="제거 권한이 없어요")
    await DB.execute("UPDATE devices SET family_id = NULL WHERE id = ?", member_device_id)
    return {"status": "ok"}


@app.get("/api/family/events")
async def get_shared_events(device_id: str, since: Optional[str] = None):
    device = await _get_device(device_id)
    if not device["family_id"]:
        return {"events": []}
    if since:
        rows = await DB.fetch(
            "SELECT * FROM shared_events WHERE family_id = ? AND updated_at > ? ORDER BY start_time",
            device["family_id"], since,
        )
    else:
        rows = await DB.fetch(
            "SELECT * FROM shared_events WHERE family_id = ? ORDER BY start_time",
            device["family_id"],
        )
    return {"events": rows}


@app.post("/api/family/events")
async def upsert_shared_event(event: SharedEventReq):
    existing = await DB.fetchrow("SELECT id FROM shared_events WHERE id = ?", event.id)
    if existing:
        await DB.execute("""
            UPDATE shared_events SET
                title=?, description=?, start_time=?, end_time=?,
                category=?, preparations=?, needs_reply=?, is_completed=?, updated_at=?
            WHERE id=?
        """, event.title, event.description, event.start_time, event.end_time,
            event.category, event.preparations, event.needs_reply, event.is_completed,
            event.updated_at, event.id)
    else:
        await DB.execute("""
            INSERT INTO shared_events
            (id, family_id, created_by, title, description, start_time, end_time,
             category, preparations, needs_reply, is_completed, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, event.id, event.family_id, event.created_by, event.title, event.description,
            event.start_time, event.end_time, event.category, event.preparations,
            event.needs_reply, event.is_completed, event.updated_at)
    return {"status": "ok"}


@app.delete("/api/family/events/{event_id}")
async def delete_shared_event(event_id: str):
    await DB.execute("DELETE FROM shared_events WHERE id = ?", event_id)
    return {"status": "ok"}


if __name__ == "__main__":
    import asyncio, socket
    try:
        local_ip = socket.gethostbyname(socket.gethostname())
    except Exception:
        local_ip = "localhost"
    port = int(os.environ.get("PORT", 8080))
    print(f"\n✅ 오늘우리집 서버 v4.0")
    print(f"   로컬: http://localhost:{port}  네트워크: http://{local_ip}:{port}")
    print(f"   DB: {'PostgreSQL' if _USE_PG else 'SQLite'}")
    uvicorn.run(app, host="0.0.0.0", port=port)
