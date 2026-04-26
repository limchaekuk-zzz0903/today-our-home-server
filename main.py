"""
오늘우리집 서버
- 로컬:    python main.py  →  sqlite:///today_our_home_server.db
- 클라우드: DATABASE_URL 환경변수 설정 시 PostgreSQL 자동 사용 (Railway/Render/Heroku)
"""
import os
import uuid
import random
import string
from contextlib import contextmanager
from datetime import datetime, timedelta
from typing import Optional

import uvicorn
from fastapi import FastAPI, HTTPException, Request, Response
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from sqlalchemy import create_engine, text

# ── DB 설정 ──────────────────────────────────────────────────────────────────

_DB_URL = os.environ.get("DATABASE_URL", "")
if _DB_URL.startswith("postgres://"):        # Railway/Heroku compat
    _DB_URL = _DB_URL.replace("postgres://", "postgresql://", 1)
if not _DB_URL:
    _DB_URL = "sqlite:///today_our_home_server.db"

engine = create_engine(_DB_URL, pool_pre_ping=True)
_IS_PG = engine.dialect.name == "postgresql"


@contextmanager
def get_db():
    with engine.connect() as conn:
        yield conn
        conn.commit()


def _one(row):
    return dict(row._mapping) if row is not None else None


def _all(rows):
    return [dict(r._mapping) for r in rows]


def init_db():
    stmts = [
        """CREATE TABLE IF NOT EXISTS users (
            id               TEXT PRIMARY KEY,
            social_provider  TEXT,
            social_id        TEXT,
            name             TEXT,
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
            host_device_id  TEXT NOT NULL,
            created_at      TEXT NOT NULL
        )""",
        """CREATE TABLE IF NOT EXISTS invite_codes (
            code        TEXT PRIMARY KEY,
            family_id   TEXT NOT NULL,
            created_by  TEXT NOT NULL,
            created_at  TEXT NOT NULL,
            expires_at  TEXT NOT NULL,
            is_used     INTEGER DEFAULT 0
        )""",
        """CREATE TABLE IF NOT EXISTS join_requests (
            id                   TEXT PRIMARY KEY,
            family_id            TEXT NOT NULL,
            requester_device_id  TEXT NOT NULL,
            requester_name       TEXT NOT NULL,
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
        # 전화번호 등록 (연락처 기반 초대용)
        """CREATE TABLE IF NOT EXISTS device_phones (
            device_id    TEXT PRIMARY KEY,
            phone_number TEXT NOT NULL,
            updated_at   TEXT NOT NULL
        )""",
        # 연락처 초대 (앱 사용자 → 앱 사용자 직접 초대)
        """CREATE TABLE IF NOT EXISTS contact_invites (
            id             TEXT PRIMARY KEY,
            family_id      TEXT NOT NULL,
            from_device_id TEXT NOT NULL,
            to_device_id   TEXT NOT NULL,
            status         TEXT DEFAULT 'pending',
            created_at     TEXT NOT NULL
        )""",
    ]
    with get_db() as conn:
        for stmt in stmts:
            conn.execute(text(stmt))
        # phone_number 유니크 인덱스 (중복 등록 방지)
        try:
            conn.execute(text(
                "CREATE UNIQUE INDEX IF NOT EXISTS idx_phone ON device_phones(phone_number)"
            ))
        except Exception:
            pass


init_db()

# ── FastAPI 앱 ────────────────────────────────────────────────────────────────

app = FastAPI(title="오늘우리집 서버", version="3.0.0")
app.add_middleware(
    CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"]
)

# ── 인증 미들웨어 ──────────────────────────────────────────────────────────────

_NO_AUTH_PATHS = {"/api/ping", "/api/auth/social"}


@app.middleware("http")
async def device_auth_middleware(request: Request, call_next):
    path = request.url.path
    if path in _NO_AUTH_PATHS or path.startswith("/docs") or path.startswith("/openapi"):
        return await call_next(request)

    device_id = request.headers.get("X-Device-ID")
    device_secret = request.headers.get("X-Device-Secret")

    if not device_id or not device_secret:
        return Response(
            content='{"detail": "인증 헤더가 없어요 (X-Device-ID, X-Device-Secret)"}',
            status_code=401,
            media_type="application/json",
        )

    with engine.connect() as conn:
        row = conn.execute(
            text("SELECT secret FROM device_secrets WHERE device_id = :did"),
            {"did": device_id},
        ).fetchone()

    if row is None or row[0] != device_secret:
        return Response(
            content='{"detail": "인증에 실패했어요. 다시 로그인해 주세요."}',
            status_code=401,
            media_type="application/json",
        )

    return await call_next(request)

# ── Pydantic 모델 ─────────────────────────────────────────────────────────────

class SocialAuthReq(BaseModel):
    provider: str
    social_id: str
    name: str
    device_id: str

class DeviceRegisterReq(BaseModel):
    id: str
    user_name: str

class PhoneRegisterReq(BaseModel):
    device_id: str
    phone_number: str

class PhoneLookupReq(BaseModel):
    phone_numbers: list[str]

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
    action: str  # 'confirm' | 'reject'

class ContactInviteReq(BaseModel):
    from_device_id: str
    to_device_id: str

class InviteRespondReq(BaseModel):
    device_id: str
    action: str  # 'accept' | 'reject'

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


def _get_device(conn, device_id: str):
    row = _one(conn.execute(
        text("SELECT * FROM devices WHERE id = :id"), {"id": device_id}
    ).fetchone())
    if not row:
        raise HTTPException(status_code=404, detail="등록되지 않은 기기예요")
    return row


def _require_family(device):
    if not device["family_id"]:
        raise HTTPException(status_code=400, detail="가족 그룹에 속해 있지 않아요")
    return device["family_id"]


def _upsert_invite_code(conn, code, family_id, created_by, created_at, expires_at):
    p = {"code": code, "fid": family_id, "by": created_by, "at": created_at, "exp": expires_at}
    if _IS_PG:
        conn.execute(text("""
            INSERT INTO invite_codes (code, family_id, created_by, created_at, expires_at, is_used)
            VALUES (:code, :fid, :by, :at, :exp, 0)
            ON CONFLICT (code) DO UPDATE SET
                family_id  = EXCLUDED.family_id,
                created_by = EXCLUDED.created_by,
                created_at = EXCLUDED.created_at,
                expires_at = EXCLUDED.expires_at,
                is_used    = 0
        """), p)
    else:
        conn.execute(text("""
            INSERT OR REPLACE INTO invite_codes
            (code, family_id, created_by, created_at, expires_at, is_used)
            VALUES (:code, :fid, :by, :at, :exp, 0)
        """), p)


def _get_family_members(conn, family_id: str):
    """가족 구성원 목록 + is_creator 플래그 반환"""
    family = _one(conn.execute(
        text("SELECT * FROM families WHERE id = :id"), {"id": family_id}
    ).fetchone())
    if not family:
        return None, []
    members_raw = _all(conn.execute(text(
        "SELECT id, user_name FROM devices WHERE family_id = :fid"
    ), {"fid": family_id}).fetchall())
    members = [
        {
            "device_id": m["id"],
            "user_name": m["user_name"],
            "is_creator": m["id"] == family["host_device_id"],
        }
        for m in members_raw
    ]
    return family, members

# ── 엔드포인트 ────────────────────────────────────────────────────────────────

@app.get("/api/ping")
def ping():
    return {"status": "ok", "message": "오늘우리집 서버 정상 동작 중"}


@app.post("/api/auth/social")
def social_auth(data: SocialAuthReq):
    """소셜 로그인 처리: 사용자 upsert + 기기 시크릿 발급"""
    with get_db() as conn:
        user = _one(conn.execute(
            text("SELECT * FROM users WHERE social_provider = :p AND social_id = :sid"),
            {"p": data.provider, "sid": data.social_id},
        ).fetchone())

        if user is None:
            user_id = str(uuid.uuid4())
            conn.execute(text(
                "INSERT INTO users (id, social_provider, social_id, name, created_at)"
                " VALUES (:id, :p, :sid, :name, :at)"
            ), {"id": user_id, "p": data.provider, "sid": data.social_id,
                "name": data.name, "at": _now()})
        else:
            user_id = user["id"]
            conn.execute(text("UPDATE users SET name = :name WHERE id = :id"),
                         {"name": data.name, "id": user_id})

        device_secret = str(uuid.uuid4())
        if _IS_PG:
            conn.execute(text("""
                INSERT INTO device_secrets (device_id, secret, user_id, created_at)
                VALUES (:did, :secret, :uid, :at)
                ON CONFLICT (device_id) DO UPDATE SET secret = EXCLUDED.secret, user_id = EXCLUDED.user_id
            """), {"did": data.device_id, "secret": device_secret,
                   "uid": user_id, "at": _now()})
        else:
            conn.execute(text("""
                INSERT OR REPLACE INTO device_secrets (device_id, secret, user_id, created_at)
                VALUES (:did, :secret, :uid, :at)
            """), {"did": data.device_id, "secret": device_secret,
                   "uid": user_id, "at": _now()})

        existing_device = _one(conn.execute(
            text("SELECT id FROM devices WHERE id = :id"), {"id": data.device_id}
        ).fetchone())
        if existing_device is None:
            conn.execute(text(
                "INSERT INTO devices (id, user_name, created_at) VALUES (:id, :name, :at)"
            ), {"id": data.device_id, "name": data.name, "at": _now()})
        else:
            conn.execute(text("UPDATE devices SET user_name = :name WHERE id = :id"),
                         {"name": data.name, "id": data.device_id})

    return {"device_secret": device_secret, "user_id": user_id}


@app.post("/api/devices/register")
def register_device(data: DeviceRegisterReq):
    with get_db() as conn:
        existing = _one(conn.execute(
            text("SELECT id FROM devices WHERE id = :id"), {"id": data.id}
        ).fetchone())
        if not existing:
            conn.execute(text(
                "INSERT INTO devices (id, user_name, created_at) VALUES (:id, :name, :at)"
            ), {"id": data.id, "name": data.user_name, "at": _now()})
        else:
            conn.execute(text(
                "UPDATE devices SET user_name = :name WHERE id = :id"
            ), {"name": data.user_name, "id": data.id})
    return {"status": "ok", "device_id": data.id}


@app.post("/api/devices/phone")
def register_phone(data: PhoneRegisterReq):
    """내 전화번호 등록 — 다른 사람이 연락처에서 나를 찾을 수 있게 함"""
    with get_db() as conn:
        # 기기 존재 확인
        _get_device(conn, data.device_id)
        p = {"did": data.device_id, "phone": data.phone_number, "at": _now()}
        if _IS_PG:
            conn.execute(text("""
                INSERT INTO device_phones (device_id, phone_number, updated_at)
                VALUES (:did, :phone, :at)
                ON CONFLICT (device_id) DO UPDATE SET phone_number = EXCLUDED.phone_number, updated_at = EXCLUDED.updated_at
            """), p)
        else:
            conn.execute(text("""
                INSERT OR REPLACE INTO device_phones (device_id, phone_number, updated_at)
                VALUES (:did, :phone, :at)
            """), p)
    return {"status": "ok"}


@app.post("/api/devices/lookup")
def lookup_contacts(data: PhoneLookupReq):
    """전화번호 목록으로 앱 사용자 조회"""
    if not data.phone_numbers:
        return {"found": []}
    with get_db() as conn:
        # SQLite/PostgreSQL 모두 호환되는 방식으로 IN 쿼리
        placeholders = ",".join([f":p{i}" for i in range(len(data.phone_numbers))])
        params = {f"p{i}": ph for i, ph in enumerate(data.phone_numbers)}
        rows = _all(conn.execute(text(f"""
            SELECT dp.phone_number, dp.device_id, d.user_name
            FROM device_phones dp
            JOIN devices d ON d.id = dp.device_id
            WHERE dp.phone_number IN ({placeholders})
        """), params).fetchall())
    return {
        "found": [
            {"phone": r["phone_number"], "device_id": r["device_id"], "user_name": r["user_name"]}
            for r in rows
        ]
    }


@app.post("/api/family/create")
def create_family(data: FamilyCreateReq):
    with get_db() as conn:
        device = _get_device(conn, data.device_id)
        if device["family_id"]:
            family = _one(conn.execute(
                text("SELECT * FROM families WHERE id = :id"), {"id": device["family_id"]}
            ).fetchone())
            return {"family_id": family["id"], "family_name": family["name"]}

        family_id = str(uuid.uuid4())
        conn.execute(text(
            "INSERT INTO families (id, name, host_device_id, created_at) VALUES (:id, :name, :host, :at)"
        ), {"id": family_id, "name": data.family_name, "host": data.device_id, "at": _now()})
        conn.execute(text(
            "UPDATE devices SET family_id = :fid WHERE id = :id"
        ), {"fid": family_id, "id": data.device_id})
    return {"family_id": family_id, "family_name": data.family_name}


@app.get("/api/family/my-families")
def get_my_families(device_id: str):
    """내가 속한 모든 가족 목록"""
    with get_db() as conn:
        device = _get_device(conn, device_id)
        if not device["family_id"]:
            return {"families": []}
        family = _one(conn.execute(
            text("SELECT * FROM families WHERE id = :id"), {"id": device["family_id"]}
        ).fetchone())
        if not family:
            return {"families": []}
        member_count = conn.execute(text(
            "SELECT COUNT(*) AS cnt FROM devices WHERE family_id = :fid"
        ), {"fid": device["family_id"]}).fetchone()[0]
    return {
        "families": [{
            "family_id": family["id"],
            "family_name": family["name"],
            "created_by": family["host_device_id"],
            "member_count": member_count,
        }]
    }


@app.post("/api/family/invite-code")
def create_invite_code(device_id: str):
    with get_db() as conn:
        device = _get_device(conn, device_id)
        family_id = _require_family(device)
        code = "".join(random.choices(string.digits, k=6))
        expires_at = (datetime.now() + timedelta(hours=24)).isoformat()
        _upsert_invite_code(conn, code, family_id, device_id, _now(), expires_at)
    return {"code": code, "expires_at": expires_at}


@app.post("/api/family/join")
def join_family(data: JoinReq):
    with get_db() as conn:
        invite = _one(conn.execute(text(
            "SELECT * FROM invite_codes WHERE code = :code AND is_used = 0 AND expires_at > :now"
        ), {"code": data.code, "now": _now()}).fetchone())
        if not invite:
            raise HTTPException(status_code=404, detail="유효하지 않은 초대 코드예요")

        device = _one(conn.execute(
            text("SELECT * FROM devices WHERE id = :id"), {"id": data.device_id}
        ).fetchone())
        if device and device["family_id"] == invite["family_id"]:
            raise HTTPException(status_code=400, detail="이미 이 가족 그룹에 속해 있어요")

        if not device:
            conn.execute(text(
                "INSERT INTO devices (id, user_name, created_at) VALUES (:id, :name, :at)"
            ), {"id": data.device_id, "name": data.device_name, "at": _now()})
        else:
            # 기기 이름 업데이트
            conn.execute(text(
                "UPDATE devices SET user_name = :name WHERE id = :id"
            ), {"name": data.device_name, "id": data.device_id})

        request_id = str(uuid.uuid4())
        conn.execute(text(
            "INSERT INTO join_requests (id, family_id, requester_device_id, requester_name, created_at)"
            " VALUES (:id, :fid, :did, :name, :at)"
        ), {"id": request_id, "fid": invite["family_id"],
            "did": data.device_id, "name": data.device_name, "at": _now()})
    return {"request_id": request_id, "status": "pending"}


@app.post("/api/family/confirm")
def confirm_join(data: ConfirmReq):
    with get_db() as conn:
        request = _one(conn.execute(
            text("SELECT * FROM join_requests WHERE id = :id"), {"id": data.request_id}
        ).fetchone())
        if not request:
            raise HTTPException(status_code=404, detail="요청을 찾을 수 없어요")
        if request["status"] != "pending":
            raise HTTPException(status_code=400, detail="이미 처리된 요청이에요")

        authorizer = _one(conn.execute(text(
            "SELECT * FROM devices WHERE id = :id AND family_id = :fid"
        ), {"id": data.device_id, "fid": request["family_id"]}).fetchone())
        if not authorizer:
            raise HTTPException(status_code=403, detail="승인 권한이 없어요")

        if data.action == "confirm":
            conn.execute(text(
                "UPDATE join_requests SET status = 'confirmed' WHERE id = :id"
            ), {"id": data.request_id})
            conn.execute(text(
                "UPDATE devices SET family_id = :fid WHERE id = :id"
            ), {"fid": request["family_id"], "id": request["requester_device_id"]})
        else:
            conn.execute(text(
                "UPDATE join_requests SET status = 'rejected' WHERE id = :id"
            ), {"id": data.request_id})
    return {"status": f"{data.action}ed"}


@app.get("/api/family/pending-requests")
def get_pending_requests(device_id: str, family_id: Optional[str] = None):
    with get_db() as conn:
        device = _get_device(conn, device_id)
        fid = family_id or device["family_id"]
        if not fid:
            return {"requests": []}
        rows = _all(conn.execute(text(
            "SELECT id, requester_device_id AS device_id, requester_name, status, created_at"
            " FROM join_requests WHERE family_id = :fid AND status = 'pending'"
        ), {"fid": fid}).fetchall())
    return {"requests": rows}


@app.get("/api/family/join-status")
def get_join_status(request_id: str):
    with get_db() as conn:
        request = _one(conn.execute(
            text("SELECT status, family_id FROM join_requests WHERE id = :id"),
            {"id": request_id}
        ).fetchone())
    if not request:
        raise HTTPException(status_code=404, detail="요청을 찾을 수 없어요")
    return {"status": request["status"], "family_id": request["family_id"]}


@app.get("/api/family/members")
def get_family_members(device_id: str):
    with get_db() as conn:
        device = _get_device(conn, device_id)
        if not device["family_id"]:
            return {"family_id": None, "family_name": None, "created_by": None, "members": []}
        family, members = _get_family_members(conn, device["family_id"])
    return {
        "family_id": device["family_id"],
        "family_name": family["name"] if family else None,
        "created_by": family["host_device_id"] if family else None,
        "members": members,
    }


@app.get("/api/family/members-v2")
def get_family_members_v2(device_id: str, family_id: Optional[str] = None):
    """구성원 목록 v2: created_by, is_creator 포함"""
    with get_db() as conn:
        device = _get_device(conn, device_id)
        fid = family_id or device["family_id"]
        if not fid:
            return {"family_id": None, "family_name": None, "created_by": None, "members": []}
        family, members = _get_family_members(conn, fid)
    return {
        "family_id": fid,
        "family_name": family["name"] if family else None,
        "created_by": family["host_device_id"] if family else None,
        "members": members,
    }


@app.post("/api/family/contact-invite")
def send_contact_invite(data: ContactInviteReq):
    """앱 사용자를 가족으로 직접 초대"""
    with get_db() as conn:
        sender = _get_device(conn, data.from_device_id)
        family_id = _require_family(sender)

        # 수신자 존재 확인
        recipient = _one(conn.execute(
            text("SELECT * FROM devices WHERE id = :id"), {"id": data.to_device_id}
        ).fetchone())
        if not recipient:
            raise HTTPException(status_code=404, detail="해당 사용자를 찾을 수 없어요")

        # 이미 같은 가족이면 스킵
        if recipient["family_id"] == family_id:
            raise HTTPException(status_code=400, detail="이미 같은 가족이에요")

        # 기존 대기 중 초대가 있으면 재사용
        existing = _one(conn.execute(text("""
            SELECT id FROM contact_invites
            WHERE family_id = :fid AND from_device_id = :from AND to_device_id = :to AND status = 'pending'
        """), {"fid": family_id, "from": data.from_device_id, "to": data.to_device_id}).fetchone())
        if existing:
            return {"status": "ok", "invite_id": existing["id"]}

        invite_id = str(uuid.uuid4())
        conn.execute(text("""
            INSERT INTO contact_invites (id, family_id, from_device_id, to_device_id, created_at)
            VALUES (:id, :fid, :from, :to, :at)
        """), {"id": invite_id, "fid": family_id,
               "from": data.from_device_id, "to": data.to_device_id, "at": _now()})
    return {"status": "ok", "invite_id": invite_id}


@app.get("/api/family/received-invitations")
def get_received_invitations(device_id: str):
    """내가 받은 초대 목록"""
    with get_db() as conn:
        rows = _all(conn.execute(text("""
            SELECT ci.id, ci.created_at,
                   d.user_name AS from_name,
                   f.name      AS family_name
            FROM contact_invites ci
            JOIN devices  d ON d.id = ci.from_device_id
            JOIN families f ON f.id = ci.family_id
            WHERE ci.to_device_id = :did AND ci.status = 'pending'
            ORDER BY ci.created_at DESC
        """), {"did": device_id}).fetchall())
    return {"invitations": [
        {"id": r["id"], "from_name": r["from_name"],
         "family_name": r["family_name"], "created_at": r["created_at"]}
        for r in rows
    ]}


@app.post("/api/family/invitations/{invite_id}/respond")
def respond_to_invitation(invite_id: str, data: InviteRespondReq):
    """초대 수락 또는 거절"""
    with get_db() as conn:
        invite = _one(conn.execute(
            text("SELECT * FROM contact_invites WHERE id = :id"), {"id": invite_id}
        ).fetchone())
        if not invite:
            raise HTTPException(status_code=404, detail="초대를 찾을 수 없어요")
        if invite["status"] != "pending":
            raise HTTPException(status_code=400, detail="이미 처리된 초대예요")
        if invite["to_device_id"] != data.device_id:
            raise HTTPException(status_code=403, detail="권한이 없어요")

        if data.action == "accept":
            conn.execute(text(
                "UPDATE contact_invites SET status = 'accepted' WHERE id = :id"
            ), {"id": invite_id})
            conn.execute(text(
                "UPDATE devices SET family_id = :fid WHERE id = :id"
            ), {"fid": invite["family_id"], "id": data.device_id})
            family = _one(conn.execute(
                text("SELECT * FROM families WHERE id = :id"), {"id": invite["family_id"]}
            ).fetchone())
            return {
                "status": "accepted",
                "family_id": invite["family_id"],
                "family_name": family["name"] if family else None,
            }
        else:
            conn.execute(text(
                "UPDATE contact_invites SET status = 'rejected' WHERE id = :id"
            ), {"id": invite_id})
            return {"status": "rejected"}


@app.delete("/api/family/group")
def delete_family(device_id: str):
    """가족 그룹 전체 삭제 (방장 전용)"""
    with get_db() as conn:
        device = _get_device(conn, device_id)
        family_id = _require_family(device)
        family = _one(conn.execute(
            text("SELECT * FROM families WHERE id = :id"), {"id": family_id}
        ).fetchone())
        if not family or family["host_device_id"] != device_id:
            raise HTTPException(status_code=403, detail="방장만 가족 그룹을 삭제할 수 있어요")

        # 모든 구성원 family_id 초기화
        conn.execute(text("UPDATE devices SET family_id = NULL WHERE family_id = :fid"), {"fid": family_id})
        # 관련 데이터 삭제
        conn.execute(text("DELETE FROM shared_events WHERE family_id = :fid"), {"fid": family_id})
        conn.execute(text("DELETE FROM invite_codes WHERE family_id = :fid"), {"fid": family_id})
        conn.execute(text("DELETE FROM join_requests WHERE family_id = :fid"), {"fid": family_id})
        conn.execute(text("DELETE FROM contact_invites WHERE family_id = :fid"), {"fid": family_id})
        conn.execute(text("DELETE FROM families WHERE id = :fid"), {"fid": family_id})
    return {"status": "ok"}


@app.patch("/api/family/members/{member_device_id}/name")
def update_member_name(member_device_id: str, data: UpdateNameReq):
    """구성원 이름 수정 (본인 또는 방장)"""
    with get_db() as conn:
        requester = _get_device(conn, data.device_id)
        target = _get_device(conn, member_device_id)

        # 같은 가족이어야 함
        if requester["family_id"] != target["family_id"]:
            raise HTTPException(status_code=403, detail="같은 가족이 아니에요")

        # 본인이거나 방장이어야 함
        family = _one(conn.execute(
            text("SELECT * FROM families WHERE id = :id"), {"id": requester["family_id"]}
        ).fetchone())
        is_admin = family and family["host_device_id"] == data.device_id
        is_self = data.device_id == member_device_id
        if not is_self and not is_admin:
            raise HTTPException(status_code=403, detail="이름 수정 권한이 없어요")

        conn.execute(text(
            "UPDATE devices SET user_name = :name WHERE id = :id"
        ), {"name": data.name, "id": member_device_id})
    return {"status": "ok"}


@app.delete("/api/family/members/{member_device_id}")
def remove_member(member_device_id: str, device_id: str):
    """구성원 제거 / 가족 나가기 (본인 또는 방장)"""
    with get_db() as conn:
        requester = _get_device(conn, device_id)
        target = _get_device(conn, member_device_id)

        if requester["family_id"] != target["family_id"]:
            raise HTTPException(status_code=403, detail="같은 가족이 아니에요")

        family = _one(conn.execute(
            text("SELECT * FROM families WHERE id = :id"), {"id": requester["family_id"]}
        ).fetchone())
        is_admin = family and family["host_device_id"] == device_id
        is_self = device_id == member_device_id

        if not is_self and not is_admin:
            raise HTTPException(status_code=403, detail="제거 권한이 없어요")

        conn.execute(text(
            "UPDATE devices SET family_id = NULL WHERE id = :id"
        ), {"id": member_device_id})
    return {"status": "ok"}


@app.get("/api/family/events")
def get_shared_events(device_id: str, since: Optional[str] = None):
    with get_db() as conn:
        device = _get_device(conn, device_id)
        if not device["family_id"]:
            return {"events": []}
        if since:
            rows = _all(conn.execute(text(
                "SELECT * FROM shared_events WHERE family_id = :fid AND updated_at > :since"
                " ORDER BY start_time"
            ), {"fid": device["family_id"], "since": since}).fetchall())
        else:
            rows = _all(conn.execute(text(
                "SELECT * FROM shared_events WHERE family_id = :fid ORDER BY start_time"
            ), {"fid": device["family_id"]}).fetchall())
    return {"events": rows}


@app.post("/api/family/events")
def upsert_shared_event(event: SharedEventReq):
    with get_db() as conn:
        existing = _one(conn.execute(
            text("SELECT id FROM shared_events WHERE id = :id"), {"id": event.id}
        ).fetchone())
        p = {
            "id": event.id, "fid": event.family_id, "by": event.created_by,
            "title": event.title, "desc": event.description,
            "st": event.start_time, "et": event.end_time,
            "cat": event.category, "prep": event.preparations,
            "nr": event.needs_reply, "ic": event.is_completed, "ua": event.updated_at,
        }
        if existing:
            conn.execute(text("""
                UPDATE shared_events SET
                    title=:title, description=:desc, start_time=:st, end_time=:et,
                    category=:cat, preparations=:prep, needs_reply=:nr,
                    is_completed=:ic, updated_at=:ua
                WHERE id=:id
            """), p)
        else:
            conn.execute(text("""
                INSERT INTO shared_events
                (id, family_id, created_by, title, description, start_time, end_time,
                 category, preparations, needs_reply, is_completed, updated_at)
                VALUES (:id, :fid, :by, :title, :desc, :st, :et, :cat, :prep, :nr, :ic, :ua)
            """), p)
    return {"status": "ok"}


@app.delete("/api/family/events/{event_id}")
def delete_shared_event(event_id: str):
    with get_db() as conn:
        conn.execute(text("DELETE FROM shared_events WHERE id = :id"), {"id": event_id})
    return {"status": "ok"}


if __name__ == "__main__":
    import socket
    try:
        local_ip = socket.gethostbyname(socket.gethostname())
    except Exception:
        local_ip = "localhost"
    port = int(os.environ.get("PORT", 8080))
    print(f"\n✅ 오늘우리집 서버 시작 v3.0")
    print(f"   로컬 주소:  http://localhost:{port}")
    print(f"   네트워크:   http://{local_ip}:{port}")
    print(f"   DB 모드:    {'PostgreSQL ☁️' if _IS_PG else 'SQLite 💾 (로컬)'}")
    print()
    uvicorn.run(app, host="0.0.0.0", port=port)
