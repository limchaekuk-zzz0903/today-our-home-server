from fastapi import FastAPI, HTTPException, Depends, Header, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import Optional, AsyncGenerator
from datetime import datetime, timedelta
import asyncio
import hashlib
import json
import os
import random
import secrets as _secrets
import string
import asyncpg

# ── 앱 초기화 ──────────────────────────────────────────────────────────────────
app = FastAPI(title="오늘우리집 API", version="1.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

DATABASE_URL = os.environ.get("DATABASE_URL", "")
_pool: asyncpg.Pool = None


async def get_db() -> AsyncGenerator[asyncpg.Connection, None]:
    """커넥션 풀에서 acquire 후 요청 완료 시 반드시 release"""
    if _pool is None:
        raise HTTPException(status_code=503, detail="데이터베이스가 연결되지 않았어요. 잠시 후 다시 시도해주세요.")
    conn = await _pool.acquire()
    try:
        yield conn
    finally:
        await _pool.release(conn)


async def _init_db():
    """DB 연결 및 테이블 생성 — 백그라운드에서 실행"""
    global _pool
    if not DATABASE_URL:
        print("⚠️  DATABASE_URL 없음 — PostgreSQL 플러그인을 추가해주세요.")
        return
    try:
        _pool = await asyncpg.create_pool(DATABASE_URL, min_size=1, max_size=10)
        print("✅ DB 연결 성공")
    except Exception as e:
        print(f"⚠️  DB 연결 실패: {e}")
        return

    async with _pool.acquire() as conn:
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS devices (
                id TEXT PRIMARY KEY,
                user_name TEXT NOT NULL DEFAULT '사용자',
                last_seen TIMESTAMPTZ DEFAULT NOW()
            );
            CREATE TABLE IF NOT EXISTS families (
                id TEXT PRIMARY KEY,
                name TEXT NOT NULL,
                created_by TEXT NOT NULL,
                created_at TIMESTAMPTZ DEFAULT NOW()
            );
            CREATE TABLE IF NOT EXISTS family_members (
                family_id TEXT NOT NULL,
                device_id TEXT NOT NULL,
                joined_at TIMESTAMPTZ DEFAULT NOW(),
                PRIMARY KEY (family_id, device_id)
            );
            CREATE TABLE IF NOT EXISTS invite_codes (
                code TEXT PRIMARY KEY,
                family_id TEXT NOT NULL,
                created_by TEXT NOT NULL,
                expires_at TIMESTAMPTZ NOT NULL,
                used BOOLEAN DEFAULT FALSE
            );
            CREATE TABLE IF NOT EXISTS join_requests (
                id TEXT PRIMARY KEY,
                family_id TEXT NOT NULL,
                device_id TEXT NOT NULL,
                device_name TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'pending',
                created_at TIMESTAMPTZ DEFAULT NOW()
            );
            CREATE TABLE IF NOT EXISTS events (
                id TEXT PRIMARY KEY,
                family_id TEXT NOT NULL,
                created_by TEXT NOT NULL,
                title TEXT NOT NULL,
                description TEXT,
                start_time TIMESTAMPTZ NOT NULL,
                end_time TIMESTAMPTZ,
                category INT DEFAULT 0,
                preparations TEXT DEFAULT '[]',
                needs_reply BOOLEAN DEFAULT FALSE,
                is_completed BOOLEAN DEFAULT FALSE,
                updated_at TIMESTAMPTZ DEFAULT NOW()
            );
            CREATE TABLE IF NOT EXISTS users (
                id TEXT PRIMARY KEY,
                email TEXT,
                name TEXT NOT NULL DEFAULT '사용자',
                provider TEXT NOT NULL DEFAULT 'email',
                social_id TEXT,
                password_hash TEXT,
                profile_image_url TEXT,
                created_at TIMESTAMPTZ DEFAULT NOW()
            );
        """)
        # 유니크 인덱스 (이미 있으면 무시)
        try:
            await conn.execute(
                "CREATE UNIQUE INDEX IF NOT EXISTS users_email_idx ON users(email) WHERE email IS NOT NULL AND provider='email'"
            )
        except Exception:
            pass
        try:
            await conn.execute(
                "CREATE UNIQUE INDEX IF NOT EXISTS users_social_idx ON users(provider, social_id) WHERE social_id IS NOT NULL"
            )
        except Exception:
            pass
    print("✅ 테이블 초기화 완료")


@app.on_event("startup")
async def startup():
    # DB 초기화를 백그라운드 태스크로 실행 → 서버가 즉시 요청 수락
    asyncio.create_task(_init_db())


@app.on_event("shutdown")
async def shutdown():
    if _pool:
        await _pool.close()


# ── 인증 헬퍼 ──────────────────────────────────────────────────────────────────

async def require_device(
    x_device_id: str = Header(..., alias="X-Device-ID"),
    conn: asyncpg.Connection = Depends(get_db),
) -> str:
    """헤더에 담긴 Device ID가 DB에 존재하는지 확인"""
    row = await conn.fetchrow("SELECT id FROM devices WHERE id=$1", x_device_id)
    if not row:
        raise HTTPException(status_code=401, detail="등록되지 않은 기기예요. 먼저 기기 등록을 해주세요.")
    await conn.execute(
        "UPDATE devices SET last_seen=NOW() WHERE id=$1", x_device_id
    )
    return x_device_id


def _rand_code(n=6) -> str:
    return "".join(random.choices(string.ascii_uppercase + string.digits, k=n))


def _rand_id(n=12) -> str:
    return "".join(random.choices(string.ascii_lowercase + string.digits, k=n))


# ── 요청/응답 모델 ─────────────────────────────────────────────────────────────

class DeviceRegisterRequest(BaseModel):
    id: str
    user_name: str = "사용자"
    user_id: Optional[str] = None


class CreateFamilyRequest(BaseModel):
    device_id: str
    family_name: str


class JoinFamilyRequest(BaseModel):
    code: str
    device_id: str
    device_name: str


class ConfirmJoinRequest(BaseModel):
    request_id: str
    device_id: str
    action: str  # "confirm" | "reject"


class EventPayload(BaseModel):
    id: str
    family_id: str
    created_by: str
    title: str
    description: Optional[str] = None
    start_time: str
    end_time: Optional[str] = None
    category: int = 0
    preparations: str = "[]"
    needs_reply: bool = False
    is_completed: bool = False
    updated_at: str


# ── Health Check ───────────────────────────────────────────────────────────────

@app.get("/")
async def root():
    return {"status": "ok", "service": "오늘우리집 API"}


@app.get("/health")
async def health():
    db_url = os.environ.get("DATABASE_URL", "")
    return {
        "status": "ok",
        "db_connected": _pool is not None,
        "db_url_set": bool(db_url),
        "db_url_prefix": db_url[:20] if db_url else "NOT SET",
    }


# ── 기기 등록 ──────────────────────────────────────────────────────────────────

@app.post("/api/devices/register")
async def register_device(
    req: DeviceRegisterRequest,
    conn: asyncpg.Connection = Depends(get_db),
):
    await conn.execute("""
        INSERT INTO devices (id, user_name, last_seen, user_id)
        VALUES ($1, $2, NOW(), $3)
        ON CONFLICT (id) DO UPDATE SET user_name=$2, last_seen=NOW(),
            user_id=COALESCE($3, devices.user_id)
    """, req.id, req.user_name, req.user_id)

    family_id = None
    family_name = None

    if req.user_id:
        # 이미 가족에 속해 있는지 확인
        existing = await conn.fetchrow(
            "SELECT family_id FROM family_members WHERE device_id=$1", req.id
        )
        if not existing:
            # 같은 user_id를 가진 다른 기기의 가족 멤버십 조회 → 이 기기에도 복원
            old_row = await conn.fetchrow("""
                SELECT fm.family_id FROM devices d
                JOIN family_members fm ON fm.device_id = d.id
                WHERE d.user_id=$1 AND d.id!=$2
                LIMIT 1
            """, req.user_id, req.id)
            if old_row:
                await conn.execute(
                    "INSERT INTO family_members (family_id, device_id) VALUES ($1,$2) ON CONFLICT DO NOTHING",
                    old_row["family_id"], req.id,
                )
                fam = await conn.fetchrow(
                    "SELECT name FROM families WHERE id=$1", old_row["family_id"]
                )
                family_id = old_row["family_id"]
                family_name = fam["name"] if fam else None

    return {"ok": True, "device_id": req.id, "family_id": family_id, "family_name": family_name}


# ── 가족 생성 ──────────────────────────────────────────────────────────────────

@app.post("/api/family/create")
async def create_family(
    req: CreateFamilyRequest,
    conn: asyncpg.Connection = Depends(get_db),
):
    # 기기 존재 확인
    device = await conn.fetchrow("SELECT id FROM devices WHERE id=$1", req.device_id)
    if not device:
        raise HTTPException(status_code=404, detail="기기를 찾을 수 없어요. 먼저 기기를 등록해주세요.")

    # 이미 가족 그룹에 속해 있으면 오류
    existing = await conn.fetchrow(
        "SELECT family_id FROM family_members WHERE device_id=$1", req.device_id
    )
    if existing:
        raise HTTPException(status_code=400, detail="이미 가족 그룹에 속해 있어요.")

    family_id = _rand_id()
    await conn.execute(
        "INSERT INTO families (id, name, created_by) VALUES ($1, $2, $3)",
        family_id, req.family_name, req.device_id,
    )
    await conn.execute(
        "INSERT INTO family_members (family_id, device_id) VALUES ($1, $2)",
        family_id, req.device_id,
    )
    return {"family_id": family_id, "family_name": req.family_name}


# ── 초대 코드 생성 ─────────────────────────────────────────────────────────────

@app.post("/api/family/invite-code")
async def generate_invite_code(
    device_id: str,
    conn: asyncpg.Connection = Depends(get_db),
):
    member = await conn.fetchrow(
        "SELECT family_id FROM family_members WHERE device_id=$1", device_id
    )
    if not member:
        raise HTTPException(status_code=403, detail="가족 그룹의 구성원만 초대 코드를 생성할 수 있어요.")

    family_id = member["family_id"]
    # 기존 유효 코드가 있으면 재사용
    existing = await conn.fetchrow(
        "SELECT code, expires_at FROM invite_codes WHERE family_id=$1 AND used=FALSE AND expires_at > NOW()",
        family_id,
    )
    if existing:
        return {"code": existing["code"], "expires_at": existing["expires_at"].isoformat()}

    code = _rand_code(6)
    expires_at = datetime.utcnow() + timedelta(hours=24)
    await conn.execute(
        "INSERT INTO invite_codes (code, family_id, created_by, expires_at) VALUES ($1, $2, $3, $4)",
        code, family_id, device_id, expires_at,
    )
    return {"code": code, "expires_at": expires_at.isoformat()}


# ── 초대 코드로 참여 요청 ──────────────────────────────────────────────────────

@app.post("/api/family/join")
async def join_family(
    req: JoinFamilyRequest,
    conn: asyncpg.Connection = Depends(get_db),
):
    # 이미 가족에 속해있는지 확인
    already = await conn.fetchrow(
        "SELECT family_id FROM family_members WHERE device_id=$1", req.device_id
    )
    if already:
        raise HTTPException(status_code=400, detail="이미 가족 그룹에 속해 있어요.")

    # 대기 중인 요청이 있는지 확인
    pending = await conn.fetchrow(
        "SELECT id FROM join_requests WHERE device_id=$1 AND status='pending'", req.device_id
    )
    if pending:
        return {"request_id": pending["id"], "status": "pending"}

    # 초대 코드 확인
    invite = await conn.fetchrow(
        "SELECT family_id FROM invite_codes WHERE code=$1 AND used=FALSE AND expires_at > NOW()",
        req.code.upper(),
    )
    if not invite:
        raise HTTPException(status_code=400, detail="유효하지 않거나 만료된 초대 코드예요.")

    request_id = _rand_id()
    await conn.execute(
        """
        INSERT INTO join_requests (id, family_id, device_id, device_name, status)
        VALUES ($1, $2, $3, $4, 'pending')
        """,
        request_id, invite["family_id"], req.device_id, req.device_name,
    )
    return {"request_id": request_id, "status": "pending"}


# ── 참여 요청 승인/거절 ────────────────────────────────────────────────────────

@app.post("/api/family/confirm")
async def confirm_join(
    req: ConfirmJoinRequest,
    conn: asyncpg.Connection = Depends(get_db),
):
    join_req = await conn.fetchrow(
        "SELECT * FROM join_requests WHERE id=$1 AND status='pending'", req.request_id
    )
    if not join_req:
        raise HTTPException(status_code=404, detail="요청을 찾을 수 없어요.")

    # 승인자가 해당 가족 구성원인지 확인
    is_member = await conn.fetchrow(
        "SELECT 1 FROM family_members WHERE device_id=$1 AND family_id=$2",
        req.device_id, join_req["family_id"],
    )
    if not is_member:
        raise HTTPException(status_code=403, detail="가족 구성원만 요청을 처리할 수 있어요.")

    if req.action == "confirm":
        await conn.execute(
            "UPDATE join_requests SET status='confirmed' WHERE id=$1", req.request_id
        )
        await conn.execute(
            "INSERT INTO family_members (family_id, device_id) VALUES ($1, $2) ON CONFLICT DO NOTHING",
            join_req["family_id"], join_req["device_id"],
        )
    else:
        await conn.execute(
            "UPDATE join_requests SET status='rejected' WHERE id=$1", req.request_id
        )

    return {"ok": True}


# ── 참여 상태 확인 ─────────────────────────────────────────────────────────────

@app.get("/api/family/join-status")
async def join_status(
    request_id: str,
    conn: asyncpg.Connection = Depends(get_db),
):
    row = await conn.fetchrow(
        "SELECT status, family_id FROM join_requests WHERE id=$1", request_id
    )
    if not row:
        raise HTTPException(status_code=404, detail="요청을 찾을 수 없어요.")
    return {"status": row["status"], "family_id": row["family_id"]}


# ── 가족 구성원 조회 ───────────────────────────────────────────────────────────

@app.get("/api/family/members")
async def get_family_members(
    device_id: str,
    conn: asyncpg.Connection = Depends(get_db),
):
    member = await conn.fetchrow(
        "SELECT family_id FROM family_members WHERE device_id=$1", device_id
    )
    if not member:
        return {"family_id": None, "family_name": None, "members": []}

    family_id = member["family_id"]
    family = await conn.fetchrow("SELECT name FROM families WHERE id=$1", family_id)
    members = await conn.fetch(
        """
        SELECT d.id, d.user_name, d.last_seen
        FROM family_members fm
        JOIN devices d ON d.id = fm.device_id
        WHERE fm.family_id = $1
        """,
        family_id,
    )
    return {
        "family_id": family_id,
        "family_name": family["name"] if family else None,
        "members": [
            {
                "device_id": m["id"],
                "user_name": m["user_name"],
                "last_seen": m["last_seen"].isoformat(),
            }
            for m in members
        ],
    }


# ── 대기 중인 참여 요청 조회 ───────────────────────────────────────────────────

@app.get("/api/family/pending-requests")
async def get_pending_requests(
    device_id: str,
    conn: asyncpg.Connection = Depends(get_db),
):
    member = await conn.fetchrow(
        "SELECT family_id FROM family_members WHERE device_id=$1", device_id
    )
    if not member:
        return {"requests": []}

    rows = await conn.fetch(
        "SELECT id, device_id, device_name, created_at FROM join_requests WHERE family_id=$1 AND status='pending'",
        member["family_id"],
    )
    return {
        "requests": [
            {
                "id": r["id"],
                "requester_name": r["device_name"],
                "device_id": r["device_id"],
                "created_at": r["created_at"].isoformat(),
            }
            for r in rows
        ]
    }


# ── 이벤트 공유 ────────────────────────────────────────────────────────────────

@app.post("/api/family/events")
async def push_event(
    payload: EventPayload,
    conn: asyncpg.Connection = Depends(get_db),
):
    await conn.execute("""
        INSERT INTO events (
            id, family_id, created_by, title, description,
            start_time, end_time, category, preparations,
            needs_reply, is_completed, updated_at
        ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)
        ON CONFLICT (id) DO UPDATE SET
            title=$4, description=$5, start_time=$6, end_time=$7,
            category=$8, preparations=$9, needs_reply=$10,
            is_completed=$11, updated_at=$12
    """,
        payload.id, payload.family_id, payload.created_by,
        payload.title, payload.description,
        datetime.fromisoformat(payload.start_time),
        datetime.fromisoformat(payload.end_time) if payload.end_time else None,
        payload.category, payload.preparations,
        payload.needs_reply, payload.is_completed,
        datetime.fromisoformat(payload.updated_at),
    )
    return {"ok": True}


@app.get("/api/family/events")
async def get_events(
    device_id: str,
    since: Optional[str] = None,
    conn: asyncpg.Connection = Depends(get_db),
):
    member = await conn.fetchrow(
        "SELECT family_id FROM family_members WHERE device_id=$1", device_id
    )
    if not member:
        return {"events": []}

    if since:
        rows = await conn.fetch(
            "SELECT * FROM events WHERE family_id=$1 AND updated_at > $2 ORDER BY start_time",
            member["family_id"], datetime.fromisoformat(since),
        )
    else:
        rows = await conn.fetch(
            "SELECT * FROM events WHERE family_id=$1 ORDER BY start_time",
            member["family_id"],
        )

    return {
        "events": [
            {
                "id": r["id"],
                "family_id": r["family_id"],
                "created_by": r["created_by"],
                "title": r["title"],
                "description": r["description"],
                "start_time": r["start_time"].isoformat(),
                "end_time": r["end_time"].isoformat() if r["end_time"] else None,
                "category": r["category"],
                "preparations": r["preparations"],
                "needs_reply": 1 if r["needs_reply"] else 0,
                "is_completed": 1 if r["is_completed"] else 0,
                "updated_at": r["updated_at"].isoformat(),
            }
            for r in rows
        ]
    }


@app.delete("/api/family/events/{event_id}")
async def delete_event(
    event_id: str,
    conn: asyncpg.Connection = Depends(get_db),
):
    await conn.execute("DELETE FROM events WHERE id=$1", event_id)
    return {"ok": True}


# ════════════════════════════════════════════════════════════════════════
# 새 기능: 전화번호 / 연락처 초대 / 소셜 인증
# ════════════════════════════════════════════════════════════════════════

# ── 새 테이블 마이그레이션 (startup에서 이미 실행됨 — 별도 함수) ──────────────

async def _migrate_contact_tables():
    """연락처 초대 기능을 위한 추가 테이블/컬럼"""
    if _pool is None:
        return
    async with _pool.acquire() as conn:
        for col_sql in [
            "ALTER TABLE devices ADD COLUMN phone_number TEXT",
            "ALTER TABLE devices ADD COLUMN user_id TEXT",
        ]:
            try:
                await conn.execute(col_sql)
            except Exception:
                pass
        try:
            await conn.execute(
                "CREATE INDEX IF NOT EXISTS devices_user_id_idx ON devices(user_id) WHERE user_id IS NOT NULL"
            )
        except Exception:
            pass
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS family_invitations (
                id TEXT PRIMARY KEY,
                family_id TEXT NOT NULL,
                from_device_id TEXT NOT NULL,
                from_name TEXT NOT NULL,
                to_device_id TEXT NOT NULL,
                to_name TEXT NOT NULL DEFAULT '사용자',
                status TEXT DEFAULT 'pending',
                created_at TIMESTAMPTZ DEFAULT NOW()
            );
            CREATE TABLE IF NOT EXISTS social_users (
                id TEXT PRIMARY KEY,
                provider TEXT NOT NULL,
                social_id TEXT NOT NULL,
                name TEXT,
                device_id TEXT,
                UNIQUE (provider, social_id)
            );
        """)
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS messages (
                id TEXT PRIMARY KEY,
                family_id TEXT NOT NULL,
                device_id TEXT NOT NULL,
                user_name TEXT NOT NULL DEFAULT '사용자',
                content TEXT NOT NULL,
                created_at TIMESTAMPTZ DEFAULT NOW()
            );
            CREATE INDEX IF NOT EXISTS messages_family_idx ON messages(family_id, created_at DESC);
        """)
    print("✅ 연락처 초대 + 메시지 테이블 마이그레이션 완료")


@app.on_event("startup")
async def startup_extended():
    # _init_db 이후 추가 마이그레이션
    await asyncio.sleep(3)           # _init_db 태스크가 먼저 끝날 때까지 대기
    asyncio.create_task(_migrate_contact_tables())


# ── 요청 모델 ─────────────────────────────────────────────────────────────────

class PhoneUpdateRequest(BaseModel):
    device_id: str
    phone_number: str

class PhoneLookupRequest(BaseModel):
    phone_numbers: list[str]

class ContactInviteRequest(BaseModel):
    from_device_id: str
    to_device_id: str

class InvitationRespondRequest(BaseModel):
    device_id: str
    action: str   # "accept" | "reject"

class SocialAuthRequest(BaseModel):
    provider: str
    social_id: str
    name: str
    device_id: str
    email: Optional[str] = None
    profile_image_url: Optional[str] = None

class EmailRegisterRequest(BaseModel):
    email: str
    password: str
    name: str
    device_id: str

class EmailLoginRequest(BaseModel):
    email: str
    password: str
    device_id: str


def _hash_pw(pw: str) -> str:
    return hashlib.sha256(pw.encode()).hexdigest()


# ── 이메일 회원가입 (서버 저장) ───────────────────────────────────────────────

@app.post("/api/auth/email/register")
async def email_register(
    req: EmailRegisterRequest,
    conn: asyncpg.Connection = Depends(get_db),
):
    existing = await conn.fetchrow(
        "SELECT id FROM users WHERE email=$1 AND provider='email'", req.email
    )
    if existing:
        raise HTTPException(status_code=400, detail="이미 사용 중인 이메일이에요.")
    uid = _rand_id()
    await conn.execute(
        "INSERT INTO users (id, email, name, provider, password_hash) VALUES ($1,$2,$3,'email',$4)",
        uid, req.email, req.name, _hash_pw(req.password),
    )
    await conn.execute("""
        INSERT INTO devices (id, user_name, user_id) VALUES ($1, $2, $3)
        ON CONFLICT (id) DO UPDATE SET user_name=$2, last_seen=NOW(), user_id=$3
    """, req.device_id, req.name, uid)
    token = _secrets.token_urlsafe(32)
    return {"user_id": uid, "name": req.name, "email": req.email, "token": token}


# ── 이메일 로그인 (서버 조회) ─────────────────────────────────────────────────

@app.post("/api/auth/email/login")
async def email_login(
    req: EmailLoginRequest,
    conn: asyncpg.Connection = Depends(get_db),
):
    row = await conn.fetchrow(
        "SELECT id, name, email, profile_image_url FROM users WHERE email=$1 AND provider='email' AND password_hash=$2",
        req.email, _hash_pw(req.password),
    )
    if not row:
        raise HTTPException(status_code=401, detail="이메일 또는 비밀번호가 일치하지 않아요.")
    await conn.execute("""
        INSERT INTO devices (id, user_name, user_id) VALUES ($1, $2, $3)
        ON CONFLICT (id) DO UPDATE SET user_name=$2, last_seen=NOW(), user_id=$3
    """, req.device_id, row["name"], row["id"])
    token = _secrets.token_urlsafe(32)
    return {
        "user_id": row["id"],
        "name": row["name"],
        "email": row["email"],
        "profile_image_url": row["profile_image_url"],
        "token": token,
    }


# ── 소셜 인증 ─────────────────────────────────────────────────────────────────

@app.post("/api/auth/social")
async def social_auth(
    req: SocialAuthRequest,
    conn: asyncpg.Connection = Depends(get_db),
):
    # users 테이블에 upsert
    row = await conn.fetchrow(
        "SELECT id, name, email FROM users WHERE provider=$1 AND social_id=$2",
        req.provider, req.social_id,
    )
    if not row:
        uid = _rand_id()
        await conn.execute(
            "INSERT INTO users (id, email, name, provider, social_id, profile_image_url) VALUES ($1,$2,$3,$4,$5,$6)",
            uid, req.email, req.name, req.provider, req.social_id, req.profile_image_url,
        )
        user_id = uid
        user_name = req.name
    else:
        user_id = row["id"]
        user_name = row["name"]

    # 레거시 social_users 테이블도 유지
    legacy = await conn.fetchrow(
        "SELECT id FROM social_users WHERE provider=$1 AND social_id=$2",
        req.provider, req.social_id,
    )
    if not legacy:
        await conn.execute(
            "INSERT INTO social_users (id, provider, social_id, name, device_id) VALUES ($1,$2,$3,$4,$5)",
            _rand_id(), req.provider, req.social_id, req.name, req.device_id,
        )

    await conn.execute("""
        INSERT INTO devices (id, user_name, user_id) VALUES ($1, $2, $3)
        ON CONFLICT (id) DO UPDATE SET user_name=$2, last_seen=NOW(), user_id=$3
    """, req.device_id, user_name, user_id)

    token = _secrets.token_urlsafe(32)
    return {"device_secret": token, "user_id": user_id, "name": user_name}


# ── 계정 복원 (앱 재설치 후 로그인 시) ────────────────────────────────────────

@app.get("/api/auth/restore")
async def restore_account(
    user_id: str,
    device_id: Optional[str] = None,
    conn: asyncpg.Connection = Depends(get_db),
):
    row = await conn.fetchrow(
        "SELECT id, email, name, provider, profile_image_url FROM users WHERE id=$1", user_id
    )
    if not row:
        raise HTTPException(status_code=404, detail="계정을 찾을 수 없어요.")

    family_id = None
    family_name = None

    # 이 user_id와 연결된 기기들 중 가족 멤버십이 있는 기기 탐색
    fam_row = await conn.fetchrow("""
        SELECT fm.family_id, f.name AS family_name
        FROM devices d
        JOIN family_members fm ON fm.device_id = d.id
        JOIN families f ON f.id = fm.family_id
        WHERE d.user_id = $1
        LIMIT 1
    """, user_id)
    if fam_row:
        family_id = fam_row["family_id"]
        family_name = fam_row["family_name"]
        # 새 device_id가 제공됐고 아직 가족에 없으면 자동 추가
        if device_id:
            await conn.execute(
                "INSERT INTO family_members (family_id, device_id) VALUES ($1,$2) ON CONFLICT DO NOTHING",
                family_id, device_id,
            )

    return {
        "user_id": row["id"],
        "email": row["email"],
        "name": row["name"],
        "provider": row["provider"],
        "profile_image_url": row["profile_image_url"],
        "family_id": family_id,
        "family_name": family_name,
    }


# ── 전화번호 등록 ──────────────────────────────────────────────────────────────

@app.post("/api/devices/phone")
async def register_phone(
    req: PhoneUpdateRequest,
    conn: asyncpg.Connection = Depends(get_db),
):
    await conn.execute("""
        INSERT INTO devices (id, phone_number) VALUES ($1, $2)
        ON CONFLICT (id) DO UPDATE SET phone_number=$2, last_seen=NOW()
    """, req.device_id, req.phone_number)
    return {"ok": True}


# ── 전화번호로 사용자 조회 ─────────────────────────────────────────────────────

@app.post("/api/devices/lookup")
async def lookup_by_phones(
    req: PhoneLookupRequest,
    conn: asyncpg.Connection = Depends(get_db),
):
    if not req.phone_numbers:
        return {"found": []}
    rows = await conn.fetch(
        "SELECT id, user_name, phone_number FROM devices WHERE phone_number = ANY($1::text[])",
        req.phone_numbers,
    )
    return {
        "found": [
            {"phone": r["phone_number"], "device_id": r["id"], "user_name": r["user_name"]}
            for r in rows
        ]
    }


# ── 연락처 초대 전송 ───────────────────────────────────────────────────────────

@app.post("/api/family/contact-invite")
async def contact_invite(
    req: ContactInviteRequest,
    conn: asyncpg.Connection = Depends(get_db),
):
    member = await conn.fetchrow(
        "SELECT family_id FROM family_members WHERE device_id=$1", req.from_device_id
    )
    if not member:
        raise HTTPException(status_code=403, detail="가족 그룹의 구성원만 초대할 수 있어요.")

    family_id = member["family_id"]

    already = await conn.fetchrow(
        "SELECT family_id FROM family_members WHERE device_id=$1", req.to_device_id
    )
    if already:
        raise HTTPException(status_code=400, detail="이미 다른 가족 그룹에 속해있어요.")

    existing = await conn.fetchrow(
        "SELECT id FROM family_invitations WHERE from_device_id=$1 AND to_device_id=$2 AND status='pending'",
        req.from_device_id, req.to_device_id,
    )
    if existing:
        raise HTTPException(status_code=400, detail="이미 초대장을 보냈어요.")

    from_dev = await conn.fetchrow("SELECT user_name FROM devices WHERE id=$1", req.from_device_id)
    to_dev   = await conn.fetchrow("SELECT user_name FROM devices WHERE id=$1", req.to_device_id)

    inv_id = _rand_id()
    await conn.execute("""
        INSERT INTO family_invitations
            (id, family_id, from_device_id, from_name, to_device_id, to_name, status)
        VALUES ($1,$2,$3,$4,$5,$6,'pending')
    """,
        inv_id, family_id,
        req.from_device_id, from_dev["user_name"] if from_dev else "사용자",
        req.to_device_id,   to_dev["user_name"]   if to_dev   else "사용자",
    )
    return {"ok": True, "invitation_id": inv_id}


# ── 받은 초대 조회 ─────────────────────────────────────────────────────────────

@app.get("/api/family/received-invitations")
async def get_received_invitations(
    device_id: str,
    conn: asyncpg.Connection = Depends(get_db),
):
    rows = await conn.fetch("""
        SELECT fi.id, fi.from_name, f.name AS family_name, fi.created_at
        FROM family_invitations fi
        JOIN families f ON f.id = fi.family_id
        WHERE fi.to_device_id=$1 AND fi.status='pending'
        ORDER BY fi.created_at DESC
    """, device_id)
    return {
        "invitations": [
            {
                "id": r["id"],
                "from_name": r["from_name"],
                "family_name": r["family_name"],
                "created_at": r["created_at"].isoformat(),
            }
            for r in rows
        ]
    }


# ── 초대 수락/거절 ─────────────────────────────────────────────────────────────

@app.post("/api/family/invitations/{inv_id}/respond")
async def respond_invitation(
    inv_id: str,
    req: InvitationRespondRequest,
    conn: asyncpg.Connection = Depends(get_db),
):
    inv = await conn.fetchrow(
        "SELECT * FROM family_invitations WHERE id=$1 AND to_device_id=$2 AND status='pending'",
        inv_id, req.device_id,
    )
    if not inv:
        raise HTTPException(status_code=404, detail="초대장을 찾을 수 없어요.")

    if req.action == "accept":
        existing = await conn.fetchrow(
            "SELECT family_id FROM family_members WHERE device_id=$1", req.device_id
        )
        if existing:
            raise HTTPException(status_code=400, detail="이미 가족 그룹에 속해 있어요.")
        await conn.execute("UPDATE family_invitations SET status='accepted' WHERE id=$1", inv_id)
        await conn.execute(
            "INSERT INTO family_members (family_id, device_id) VALUES ($1,$2) ON CONFLICT DO NOTHING",
            inv["family_id"], req.device_id,
        )
        family = await conn.fetchrow("SELECT name FROM families WHERE id=$1", inv["family_id"])
        return {"ok": True, "family_id": inv["family_id"], "family_name": family["name"] if family else "우리 가족"}
    else:
        await conn.execute("UPDATE family_invitations SET status='rejected' WHERE id=$1", inv_id)
        return {"ok": True}


# ── 가족 구성원 조회 (created_by 포함) ────────────────────────────────────────

@app.get("/api/family/members-v2")
async def get_family_members_v2(
    device_id: str,
    conn: asyncpg.Connection = Depends(get_db),
):
    member = await conn.fetchrow(
        "SELECT family_id FROM family_members WHERE device_id=$1", device_id
    )
    if not member:
        return {"family_id": None, "family_name": None, "members": [], "created_by": None}

    family_id = member["family_id"]
    family = await conn.fetchrow("SELECT name, created_by FROM families WHERE id=$1", family_id)
    members = await conn.fetch("""
        SELECT d.id, d.user_name, d.last_seen
        FROM family_members fm
        JOIN devices d ON d.id = fm.device_id
        WHERE fm.family_id=$1
        ORDER BY fm.joined_at
    """, family_id)
    created_by = family["created_by"] if family else None
    return {
        "family_id": family_id,
        "family_name": family["name"] if family else None,
        "created_by": created_by,
        "members": [
            {
                "device_id": m["id"],
                "user_name": m["user_name"],
                "last_seen": m["last_seen"].isoformat(),
                "is_creator": m["id"] == created_by,
            }
            for m in members
        ],
    }


# ── 가족 구성원 제거 / 나가기 ─────────────────────────────────────────────────

@app.delete("/api/family/members/{member_device_id}")
async def remove_family_member(
    member_device_id: str,
    device_id: str,
    conn: asyncpg.Connection = Depends(get_db),
):
    requester = await conn.fetchrow(
        "SELECT family_id FROM family_members WHERE device_id=$1", device_id
    )
    if not requester:
        raise HTTPException(status_code=403, detail="가족 그룹의 구성원이 아니에요.")

    family_id = requester["family_id"]
    target = await conn.fetchrow(
        "SELECT 1 FROM family_members WHERE family_id=$1 AND device_id=$2",
        family_id, member_device_id,
    )
    if not target:
        raise HTTPException(status_code=404, detail="해당 구성원을 찾을 수 없어요.")

    family = await conn.fetchrow("SELECT created_by FROM families WHERE id=$1", family_id)
    is_creator = family and family["created_by"] == device_id
    is_self    = device_id == member_device_id

    if not (is_creator or is_self):
        raise HTTPException(status_code=403, detail="방장만 다른 구성원을 제거할 수 있어요.")

    await conn.execute(
        "DELETE FROM family_members WHERE family_id=$1 AND device_id=$2",
        family_id, member_device_id,
    )
    # 자신이 나가는 경우 대기 중인 초대 취소
    if is_self:
        await conn.execute(
            "UPDATE family_invitations SET status='rejected' WHERE to_device_id=$1 AND status='pending'",
            member_device_id,
        )
    return {"ok": True}


@app.patch("/api/family/members/{member_device_id}/name")
async def update_member_name(
    member_device_id: str,
    body: dict,
    conn: asyncpg.Connection = Depends(get_db),
):
    requester_device_id = body.get("device_id")
    new_name = (body.get("name") or "").strip()
    if not new_name:
        raise HTTPException(status_code=400, detail="이름을 입력해주세요.")

    requester = await conn.fetchrow(
        "SELECT family_id FROM family_members WHERE device_id=$1", requester_device_id
    )
    if not requester:
        raise HTTPException(status_code=403, detail="가족 그룹의 구성원이 아니에요.")

    family_id = requester["family_id"]
    target = await conn.fetchrow(
        "SELECT 1 FROM family_members WHERE family_id=$1 AND device_id=$2",
        family_id, member_device_id,
    )
    if not target:
        raise HTTPException(status_code=404, detail="해당 구성원을 찾을 수 없어요.")

    family = await conn.fetchrow("SELECT created_by FROM families WHERE id=$1", family_id)
    is_creator = family and family["created_by"] == requester_device_id
    is_self = requester_device_id == member_device_id

    if not (is_creator or is_self):
        raise HTTPException(status_code=403, detail="방장만 다른 구성원의 이름을 수정할 수 있어요.")

    await conn.execute(
        "UPDATE devices SET user_name=$1 WHERE id=$2", new_name, member_device_id
    )
    return {"ok": True, "name": new_name}


# ── 가족 그룹 전체 삭제 (방장 전용) ────────────────────────────────────────────

@app.delete("/api/family/group")
async def delete_family_group(
    device_id: str,
    conn: asyncpg.Connection = Depends(get_db),
):
    member = await conn.fetchrow(
        "SELECT family_id FROM family_members WHERE device_id=$1", device_id
    )
    if not member:
        raise HTTPException(status_code=404, detail="가족 그룹을 찾을 수 없어요.")

    family_id = member["family_id"]
    family = await conn.fetchrow("SELECT created_by FROM families WHERE id=$1", family_id)
    if not family or family["created_by"] != device_id:
        raise HTTPException(status_code=403, detail="방장만 가족 그룹을 삭제할 수 있어요.")

    # 관련 데이터 모두 삭제 (cascade)
    await conn.execute("DELETE FROM family_members WHERE family_id=$1", family_id)
    await conn.execute("DELETE FROM invite_codes WHERE family_id=$1", family_id)
    await conn.execute("DELETE FROM join_requests WHERE family_id=$1", family_id)
    await conn.execute("DELETE FROM events WHERE family_id=$1", family_id)
    await conn.execute(
        "DELETE FROM family_invitations WHERE family_id=$1", family_id
    )
    await conn.execute("DELETE FROM messages WHERE family_id=$1", family_id)
    await conn.execute("DELETE FROM families WHERE id=$1", family_id)
    return {"ok": True}


# ── 개발/초기화용: 특정 기기의 가족 데이터 초기화 ──────────────────────────────

@app.post("/api/dev/reset-device")
async def dev_reset_device(
    body: dict,
    conn: asyncpg.Connection = Depends(get_db),
):
    secret = body.get("secret", "")
    if secret != "todayourhome-reset-2024":
        raise HTTPException(status_code=403, detail="unauthorized")

    device_id = body.get("device_id")
    if device_id:
        # 특정 기기만 초기화
        member = await conn.fetchrow(
            "SELECT family_id FROM family_members WHERE device_id=$1", device_id
        )
        if member:
            family_id = member["family_id"]
            family = await conn.fetchrow("SELECT created_by FROM families WHERE id=$1", family_id)
            if family and family["created_by"] == device_id:
                # 방장 기기 → 그룹 전체 삭제
                await conn.execute("DELETE FROM family_members WHERE family_id=$1", family_id)
                await conn.execute("DELETE FROM invite_codes WHERE family_id=$1", family_id)
                await conn.execute("DELETE FROM join_requests WHERE family_id=$1", family_id)
                await conn.execute("DELETE FROM events WHERE family_id=$1", family_id)
                await conn.execute("DELETE FROM messages WHERE family_id=$1", family_id)
                await conn.execute("DELETE FROM families WHERE id=$1", family_id)
            else:
                # 일반 구성원 → 해당 기기만 제거
                await conn.execute(
                    "DELETE FROM family_members WHERE device_id=$1", device_id
                )
        await conn.execute("DELETE FROM join_requests WHERE device_id=$1", device_id)
        await conn.execute(
            "UPDATE invite_codes SET used=TRUE WHERE created_by=$1", device_id
        )
        return {"ok": True, "reset": "device", "device_id": device_id}
    else:
        # 전체 초기화 (family 관련 테이블만)
        await conn.execute("DELETE FROM messages WHERE 1=1")
        await conn.execute("DELETE FROM family_invitations WHERE 1=1")
        await conn.execute("DELETE FROM family_members WHERE 1=1")
        await conn.execute("DELETE FROM invite_codes WHERE 1=1")
        await conn.execute("DELETE FROM join_requests WHERE 1=1")
        await conn.execute("DELETE FROM events WHERE 1=1")
        await conn.execute("DELETE FROM families WHERE 1=1")
        return {"ok": True, "reset": "all"}


# ── WebSocket 연결 관리자 ──────────────────────────────────────────────────────

class ChatConnectionManager:
    def __init__(self):
        # family_id → set of WebSocket
        self._rooms: dict[str, set[WebSocket]] = {}

    def _room(self, family_id: str) -> set[WebSocket]:
        if family_id not in self._rooms:
            self._rooms[family_id] = set()
        return self._rooms[family_id]

    async def connect(self, ws: WebSocket, family_id: str):
        await ws.accept()
        self._room(family_id).add(ws)

    def disconnect(self, ws: WebSocket, family_id: str):
        self._room(family_id).discard(ws)

    async def broadcast(self, family_id: str, message: dict, exclude: WebSocket | None = None):
        text = json.dumps(message, ensure_ascii=False)
        dead = set()
        for ws in list(self._room(family_id)):
            if ws is exclude:
                continue
            try:
                await ws.send_text(text)
            except Exception:
                dead.add(ws)
        for ws in dead:
            self._room(family_id).discard(ws)


_chat_mgr = ChatConnectionManager()


# ── WebSocket 채팅 ─────────────────────────────────────────────────────────────

@app.websocket("/ws/chat/{device_id}")
async def ws_chat(device_id: str, ws: WebSocket):
    if _pool is None:
        await ws.close(code=1011)
        return

    async with _pool.acquire() as conn:
        member = await conn.fetchrow(
            "SELECT family_id FROM family_members WHERE device_id=$1", device_id
        )
        if not member:
            await ws.close(code=4003)
            return
        family_id = member["family_id"]
        device = await conn.fetchrow("SELECT user_name FROM devices WHERE id=$1", device_id)
        user_name = device["user_name"] if device else "사용자"

        # 최근 50개 메시지 히스토리 전송
        rows = await conn.fetch("""
            SELECT id, device_id, user_name, content, created_at
            FROM messages WHERE family_id=$1
            ORDER BY created_at DESC LIMIT 50
        """, family_id)
        history = [
            {
                "type": "history",
                "id": r["id"],
                "device_id": r["device_id"],
                "user_name": r["user_name"],
                "content": r["content"],
                "created_at": r["created_at"].isoformat(),
            }
            for r in reversed(rows)
        ]

    await _chat_mgr.connect(ws, family_id)
    try:
        # 히스토리 일괄 전송
        await ws.send_text(json.dumps({"type": "history_batch", "messages": history}, ensure_ascii=False))

        while True:
            data = await ws.receive_text()
            try:
                payload = json.loads(data)
            except Exception:
                continue

            content = (payload.get("content") or "").strip()
            if not content:
                continue

            async with _pool.acquire() as conn:
                msg_id = _rand_id()
                await conn.execute("""
                    INSERT INTO messages (id, family_id, device_id, user_name, content)
                    VALUES ($1, $2, $3, $4, $5)
                """, msg_id, family_id, device_id, user_name, content)

                created_at = datetime.utcnow().isoformat()

            msg = {
                "type": "message",
                "id": msg_id,
                "device_id": device_id,
                "user_name": user_name,
                "content": content,
                "created_at": created_at,
            }
            # 보낸 사람 포함 모든 구성원에게 전송
            await _chat_mgr.broadcast(family_id, msg)

    except WebSocketDisconnect:
        pass
    finally:
        _chat_mgr.disconnect(ws, family_id)


# ── 메신저 ─────────────────────────────────────────────────────────────────────

class SendMessageRequest(BaseModel):
    device_id: str
    content: str

@app.post("/api/family/messages")
async def send_message(
    req: SendMessageRequest,
    conn: asyncpg.Connection = Depends(get_db),
):
    member = await conn.fetchrow(
        "SELECT family_id FROM family_members WHERE device_id=$1", req.device_id
    )
    if not member:
        raise HTTPException(status_code=403, detail="가족 그룹의 구성원만 메시지를 보낼 수 있어요.")

    device = await conn.fetchrow("SELECT user_name FROM devices WHERE id=$1", req.device_id)
    user_name = device["user_name"] if device else "사용자"

    msg_id = _rand_id()
    await conn.execute("""
        INSERT INTO messages (id, family_id, device_id, user_name, content)
        VALUES ($1, $2, $3, $4, $5)
    """, msg_id, member["family_id"], req.device_id, user_name, req.content)

    return {"ok": True, "id": msg_id}


@app.get("/api/family/messages")
async def get_messages(
    device_id: str,
    since: Optional[str] = None,
    limit: int = 50,
    conn: asyncpg.Connection = Depends(get_db),
):
    member = await conn.fetchrow(
        "SELECT family_id FROM family_members WHERE device_id=$1", device_id
    )
    if not member:
        return {"messages": []}

    if since:
        rows = await conn.fetch("""
            SELECT id, device_id, user_name, content, created_at
            FROM messages
            WHERE family_id=$1 AND created_at > $2
            ORDER BY created_at ASC
        """, member["family_id"], datetime.fromisoformat(since))
    else:
        rows = await conn.fetch("""
            SELECT id, device_id, user_name, content, created_at
            FROM messages
            WHERE family_id=$1
            ORDER BY created_at DESC
            LIMIT $2
        """, member["family_id"], limit)
        rows = list(reversed(rows))

    return {
        "messages": [
            {
                "id": r["id"],
                "device_id": r["device_id"],
                "user_name": r["user_name"],
                "content": r["content"],
                "created_at": r["created_at"].isoformat(),
            }
            for r in rows
        ]
    }
