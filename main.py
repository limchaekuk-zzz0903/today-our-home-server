from fastapi import FastAPI, HTTPException, Depends, Header
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import Optional
from datetime import datetime, timedelta
import os
import random
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


async def get_db() -> asyncpg.Connection:
    if _pool is None:
        raise HTTPException(status_code=503, detail="데이터베이스가 연결되지 않았어요. 잠시 후 다시 시도해주세요.")
    return await _pool.acquire()


@app.on_event("startup")
async def startup():
    global _pool
    if not DATABASE_URL:
        print("⚠️  DATABASE_URL 환경변수가 없습니다. Railway에서 PostgreSQL 플러그인을 추가해주세요.")
        return
    try:
        _pool = await asyncpg.create_pool(DATABASE_URL, min_size=1, max_size=10)
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
        """)


@app.on_event("shutdown")
async def shutdown():
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
    return {"status": "ok"}


# ── 기기 등록 ──────────────────────────────────────────────────────────────────

@app.post("/api/devices/register")
async def register_device(
    req: DeviceRegisterRequest,
    conn: asyncpg.Connection = Depends(get_db),
):
    await conn.execute("""
        INSERT INTO devices (id, user_name, last_seen)
        VALUES ($1, $2, NOW())
        ON CONFLICT (id) DO UPDATE SET user_name=$2, last_seen=NOW()
    """, req.id, req.user_name)
    return {"ok": True, "device_id": req.id}


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
