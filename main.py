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
import json
import asyncio
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional, Set, Tuple

import asyncpg
import bcrypt
import uvicorn
from fastapi import FastAPI, HTTPException, Query, Request, Response, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, HTMLResponse
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
    """CREATE TABLE IF NOT EXISTS chat_messages (
        id         TEXT PRIMARY KEY,
        family_id  TEXT NOT NULL,
        device_id  TEXT NOT NULL,
        user_name  TEXT NOT NULL,
        content    TEXT NOT NULL,
        created_at TEXT NOT NULL
    )""",
    "CREATE INDEX IF NOT EXISTS idx_chat_messages_family_created ON chat_messages (family_id, created_at)",
    # ── 미션 & 포상 (가족 게이미피케이션) ──────────────────────────────────────
    """CREATE TABLE IF NOT EXISTS missions (
        id                  TEXT PRIMARY KEY,
        family_id           TEXT NOT NULL,
        created_by          TEXT NOT NULL,
        title               TEXT NOT NULL,
        description         TEXT,
        reward_points       INTEGER NOT NULL DEFAULT 0,
        assignee_device_id  TEXT,
        icon                TEXT,
        require_approval    INTEGER DEFAULT 1,
        recurrence          TEXT DEFAULT 'none',
        archived            INTEGER DEFAULT 0,
        created_at          TEXT NOT NULL
    )""",
    "CREATE INDEX IF NOT EXISTS idx_missions_family ON missions (family_id, archived)",
    """CREATE TABLE IF NOT EXISTS mission_completions (
        id              TEXT PRIMARY KEY,
        mission_id      TEXT NOT NULL,
        family_id       TEXT NOT NULL,
        user_device_id  TEXT NOT NULL,
        user_name       TEXT NOT NULL,
        status          TEXT DEFAULT 'pending',
        points_awarded  INTEGER NOT NULL DEFAULT 0,
        approved_by     TEXT,
        completed_at    TEXT NOT NULL,
        approved_at     TEXT
    )""",
    "CREATE INDEX IF NOT EXISTS idx_mc_family_status ON mission_completions (family_id, status)",
    """CREATE TABLE IF NOT EXISTS rewards (
        id           TEXT PRIMARY KEY,
        family_id    TEXT NOT NULL,
        created_by   TEXT NOT NULL,
        title        TEXT NOT NULL,
        description  TEXT,
        cost_points  INTEGER NOT NULL,
        icon         TEXT,
        stock        INTEGER,
        archived     INTEGER DEFAULT 0,
        created_at   TEXT NOT NULL
    )""",
    "CREATE INDEX IF NOT EXISTS idx_rewards_family ON rewards (family_id, archived)",
    """CREATE TABLE IF NOT EXISTS reward_redemptions (
        id              TEXT PRIMARY KEY,
        reward_id       TEXT NOT NULL,
        family_id       TEXT NOT NULL,
        user_device_id  TEXT NOT NULL,
        user_name       TEXT NOT NULL,
        status          TEXT DEFAULT 'pending',
        points_cost     INTEGER NOT NULL,
        approved_by     TEXT,
        requested_at    TEXT NOT NULL,
        approved_at     TEXT
    )""",
    "CREATE INDEX IF NOT EXISTS idx_rr_family_status ON reward_redemptions (family_id, status)",
    # 가족 단위 멤버별 포인트 잔액 — 서버 진실의 원천
    # ── 목표 포인트 ───────────────────────────────────────────────────────────
    # 스티커판(포도알)과 짝을 이루는 포인트 쪽 목표. 리더가 정한다.
    """CREATE TABLE IF NOT EXISTS point_goals (
        family_id    TEXT PRIMARY KEY,
        goal_points  INTEGER NOT NULL DEFAULT 100,
        reward_title TEXT,
        updated_at   TEXT NOT NULL
    )""",

    # ── 소원 티켓 ─────────────────────────────────────────────────────────────
    # "이제 소원 하나 고를 수 있다"는 권리를 명시적으로 남긴다.
    # 스티커판 완성 / 목표 포인트 도달 / 목표 미션 성공 — 세 곳에서 발급된다.
    # 티켓 없이 아무 때나 소원을 조를 수 있으면 목표를 세우는 의미가 없다.
    """CREATE TABLE IF NOT EXISTS wish_tickets (
        id           TEXT PRIMARY KEY,
        family_id    TEXT NOT NULL,
        device_id    TEXT NOT NULL,
        source       TEXT NOT NULL,
        source_id    TEXT,
        label        TEXT,
        used_grant_id TEXT,
        created_at   TEXT NOT NULL,
        used_at      TEXT
    )""",
    "CREATE INDEX IF NOT EXISTS idx_tickets_owner ON wish_tickets (family_id, device_id, used_at)",
    "CREATE UNIQUE INDEX IF NOT EXISTS idx_tickets_source ON wish_tickets (family_id, device_id, source, source_id)",

    # ── 리더 권한 위임 ─────────────────────────────────────────────────────────
    # 리더는 여러 명일 수 있다(부모 둘). 권한은 주는 쪽이 요청하고 받는 쪽이 수락한다 —
    # 통보만으로 권한이 바뀌면 본인도 모르게 승인 책임을 떠안게 된다.
    """CREATE TABLE IF NOT EXISTS role_change_requests (
        id            TEXT PRIMARY KEY,
        family_id     TEXT NOT NULL,
        target_device_id TEXT NOT NULL,
        requested_by  TEXT NOT NULL,
        requester_name TEXT,
        to_role       TEXT NOT NULL,
        status        TEXT NOT NULL DEFAULT 'pending',
        created_at    TEXT NOT NULL,
        resolved_at   TEXT
    )""",
    "CREATE INDEX IF NOT EXISTS idx_rolereq_target ON role_change_requests (target_device_id, status)",

    # ── 소원 리스트 ───────────────────────────────────────────────────────────
    # 보상을 부모가 정해주면 아이가 원하지 않는 걸 받게 된다.
    # 각자 자기가 받고 싶은 걸 적어두고, 거기서 고른다.
    """CREATE TABLE IF NOT EXISTS member_wishes (
        id          TEXT PRIMARY KEY,
        family_id   TEXT NOT NULL,
        device_id   TEXT NOT NULL,
        title       TEXT NOT NULL,
        emoji       TEXT,
        note        TEXT,
        archived    INTEGER NOT NULL DEFAULT 0,
        created_at  TEXT NOT NULL
    )""",
    "CREATE INDEX IF NOT EXISTS idx_wishes_owner ON member_wishes (family_id, device_id, archived)",
    # 소원 지급 — 아이가 요청하거나(requested), 부모가 먼저 약속한다(promised).
    """CREATE TABLE IF NOT EXISTS wish_grants (
        id            TEXT PRIMARY KEY,
        family_id     TEXT NOT NULL,
        wish_id       TEXT NOT NULL,
        wish_title    TEXT,
        owner_device_id TEXT NOT NULL,
        owner_name    TEXT,
        status        TEXT NOT NULL DEFAULT 'requested',
        source        TEXT,
        note          TEXT,
        requested_by  TEXT,
        decided_by    TEXT,
        created_at    TEXT NOT NULL,
        updated_at    TEXT
    )""",
    "CREATE INDEX IF NOT EXISTS idx_wishgrants_family ON wish_grants (family_id, status)",

    # ── 위치 공유 ──────────────────────────────────────────────────────────────
    # 상시 추적이 아니라 "안전 구역 도착·출발"과 "요청 시 1회 응답"만 다룬다.
    # 상시 GPS는 배터리를 태우고 Play 심사도 통과하기 어렵다.
    """CREATE TABLE IF NOT EXISTS family_places (
        id          TEXT PRIMARY KEY,
        family_id   TEXT NOT NULL,
        name        TEXT NOT NULL,
        emoji       TEXT,
        lat         REAL NOT NULL,
        lng         REAL NOT NULL,
        radius_m    INTEGER NOT NULL DEFAULT 150,
        created_by  TEXT NOT NULL,
        created_at  TEXT NOT NULL
    )""",
    "CREATE INDEX IF NOT EXISTS idx_places_family ON family_places (family_id)",
    # 위치 공유 동의 — 동의한 적 없으면 어떤 위치도 받지 않는다.
    """CREATE TABLE IF NOT EXISTS location_consents (
        family_id   TEXT NOT NULL,
        device_id   TEXT NOT NULL,
        agreed      INTEGER NOT NULL DEFAULT 0,
        paused      INTEGER NOT NULL DEFAULT 0,
        agreed_at   TEXT,
        updated_at  TEXT NOT NULL,
        PRIMARY KEY (family_id, device_id)
    )""",
    # 도착/출발 로그 — 좌표가 아니라 "어느 장소에" 를 남긴다.
    """CREATE TABLE IF NOT EXISTS location_events (
        id          TEXT PRIMARY KEY,
        family_id   TEXT NOT NULL,
        device_id   TEXT NOT NULL,
        user_name   TEXT,
        place_id    TEXT,
        place_name  TEXT,
        kind        TEXT NOT NULL,
        lat         REAL,
        lng         REAL,
        occurred_at TEXT NOT NULL
    )""",
    "CREATE INDEX IF NOT EXISTS idx_locevents_family ON location_events (family_id, occurred_at)",
    # "지금 어디야?" 요청과 응답 — 조회 사실이 반드시 기록된다.
    """CREATE TABLE IF NOT EXISTS location_pings (
        id              TEXT PRIMARY KEY,
        family_id       TEXT NOT NULL,
        requested_by    TEXT NOT NULL,
        requester_name  TEXT,
        target_device_id TEXT NOT NULL,
        status          TEXT NOT NULL DEFAULT 'pending',
        lat             REAL,
        lng             REAL,
        accuracy_m      REAL,
        requested_at    TEXT NOT NULL,
        answered_at     TEXT
    )""",
    "CREATE INDEX IF NOT EXISTS idx_pings_target ON location_pings (target_device_id, status)",

    # ── 일정 완료 승인 대기 ────────────────────────────────────────────────────
    # 아이가 일정을 완료하면 바로 포인트를 주지 않고 부모 확인을 거친다.
    # 미션(mission_completions)과 같은 모양이라 승인 화면에서 함께 다룰 수 있다.
    """CREATE TABLE IF NOT EXISTS event_completions (
        id              TEXT PRIMARY KEY,
        family_id       TEXT NOT NULL,
        event_id        TEXT NOT NULL,
        event_title     TEXT,
        user_device_id  TEXT NOT NULL,
        user_name       TEXT,
        status          TEXT NOT NULL DEFAULT 'pending',
        points_awarded  INTEGER NOT NULL DEFAULT 0,
        requested_points INTEGER NOT NULL DEFAULT 0,
        approved_by     TEXT,
        reject_reason   TEXT,
        completed_at    TEXT NOT NULL,
        approved_at     TEXT
    )""",
    "CREATE INDEX IF NOT EXISTS idx_event_completions_family ON event_completions (family_id, status)",
    "CREATE UNIQUE INDEX IF NOT EXISTS idx_event_completions_unique ON event_completions (family_id, event_id, user_device_id)",
    """CREATE TABLE IF NOT EXISTS family_points (
        family_id   TEXT NOT NULL,
        device_id   TEXT NOT NULL,
        balance     INTEGER NOT NULL DEFAULT 0,
        updated_at  TEXT NOT NULL,
        PRIMARY KEY (family_id, device_id)
    )""",
    # ── 스티커판 (개인별 목표판 → 목표 달성 시 소원권 자동 지급) ─────────────────────
    """CREATE TABLE IF NOT EXISTS sticker_board_configs (
        family_id           TEXT PRIMARY KEY,
        goal_count          INTEGER NOT NULL DEFAULT 10,
        reward_title        TEXT NOT NULL DEFAULT '소원권',
        reward_description  TEXT,
        updated_by          TEXT,
        updated_at          TEXT NOT NULL
    )""",
    """CREATE TABLE IF NOT EXISTS member_stickers (
        id          TEXT PRIMARY KEY,
        family_id   TEXT NOT NULL,
        device_id   TEXT NOT NULL,
        source      TEXT NOT NULL,
        source_id   TEXT NOT NULL,
        consumed    INTEGER NOT NULL DEFAULT 0,
        created_at  TEXT NOT NULL
    )""",
    "CREATE INDEX IF NOT EXISTS idx_member_stickers_lookup ON member_stickers (family_id, device_id, consumed)",
    """CREATE TABLE IF NOT EXISTS sticker_board_completions (
        id                  TEXT PRIMARY KEY,
        family_id           TEXT NOT NULL,
        device_id           TEXT NOT NULL,
        user_name           TEXT NOT NULL,
        goal_count          INTEGER NOT NULL,
        reward_title        TEXT NOT NULL,
        reward_description  TEXT,
        completed_at        TEXT NOT NULL
    )""",
    "CREATE INDEX IF NOT EXISTS idx_sticker_completions_family ON sticker_board_completions (family_id, completed_at)",
    # ── 공유 리스트 (장보기 / 할일 / 준비물) ─────────────────────────────────────
    """CREATE TABLE IF NOT EXISTS family_lists (
        id          TEXT PRIMARY KEY,
        family_id   TEXT NOT NULL,
        created_by  TEXT NOT NULL,
        title       TEXT NOT NULL,
        type        TEXT DEFAULT 'checklist',
        icon        TEXT,
        archived    INTEGER DEFAULT 0,
        created_at  TEXT NOT NULL,
        updated_at  TEXT NOT NULL
    )""",
    "CREATE INDEX IF NOT EXISTS idx_family_lists_family ON family_lists (family_id, archived)",
    """CREATE TABLE IF NOT EXISTS family_list_items (
        id               TEXT PRIMARY KEY,
        list_id          TEXT NOT NULL,
        family_id        TEXT NOT NULL,
        content          TEXT NOT NULL,
        quantity         TEXT,
        is_checked       INTEGER DEFAULT 0,
        checked_by       TEXT,
        checked_by_name  TEXT,
        created_by       TEXT NOT NULL,
        position         INTEGER DEFAULT 0,
        created_at       TEXT NOT NULL,
        updated_at       TEXT NOT NULL
    )""",
    "CREATE INDEX IF NOT EXISTS idx_family_list_items_list ON family_list_items (list_id, position)",
    # ── 이벤트 코멘트 (일정별 가족 대화, Phase 4) ────────────────────────────────
    """CREATE TABLE IF NOT EXISTS event_comments (
        id          TEXT PRIMARY KEY,
        event_id    TEXT NOT NULL,
        family_id   TEXT NOT NULL,
        device_id   TEXT NOT NULL,
        user_name   TEXT NOT NULL,
        content     TEXT NOT NULL,
        created_at  TEXT NOT NULL
    )""",
    "CREATE INDEX IF NOT EXISTS idx_event_comments ON event_comments (family_id, event_id, created_at)",
    # ── 공유 일정 확인(읽음) 기록 — 누가 이 일정을 열어 봤는지 ──────────────────
    # 기기당 최초 1회만 남긴다(PK). seen_at은 처음 확인한 시각.
    """CREATE TABLE IF NOT EXISTS event_views (
        event_id    TEXT NOT NULL,
        family_id   TEXT NOT NULL,
        device_id   TEXT NOT NULL,
        user_name   TEXT NOT NULL,
        seen_at     TEXT NOT NULL,
        PRIMARY KEY (event_id, device_id)
    )""",
    "CREATE INDEX IF NOT EXISTS idx_event_views ON event_views (family_id, event_id)",
    # ── 주간 식단 플래너 (Phase 4) ──────────────────────────────────────────────
    """CREATE TABLE IF NOT EXISTS meal_plans (
        id          TEXT PRIMARY KEY,
        family_id   TEXT NOT NULL,
        date        TEXT NOT NULL,
        meal_type   TEXT NOT NULL DEFAULT 'dinner',
        title       TEXT NOT NULL,
        memo        TEXT,
        created_by  TEXT NOT NULL,
        created_at  TEXT NOT NULL,
        updated_at  TEXT NOT NULL
    )""",
    "CREATE INDEX IF NOT EXISTS idx_meal_plans ON meal_plans (family_id, date)",
    # ── 숫자섬 수학 탐험대(/learn/) ─────────────────────────────────────────────
    # 웹뷰는 요청마다 X-Device 헤더를 붙일 수 없어, 첫 로드 때 헤더로 확인하고
    # 이 토큰을 HttpOnly 쿠키로 준다. 기기 비밀이 바뀌면(재로그인) 토큰도 무효.
    """CREATE TABLE IF NOT EXISTS learn_sessions (
        token       TEXT PRIMARY KEY,
        device_id   TEXT NOT NULL,
        user_id     TEXT NOT NULL,
        expires_at  BIGINT NOT NULL
    )""",
    # 진도는 사람(user_id) 기준 — 폰을 바꿔도 별과 크리스탈이 남아야 한다.
    """CREATE TABLE IF NOT EXISTS learn_progress (
        user_id     TEXT PRIMARY KEY,
        progress    TEXT NOT NULL,
        updated_at  TEXT NOT NULL
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
        # missions 반복 옵션 (Phase 2)
        "ALTER TABLE missions ADD COLUMN recurrence TEXT DEFAULT 'none'",
        # 보상 분리: 미션은 포도알, 일정은 포인트.
        # reward_points는 과거 기록 보존을 위해 남겨두고 더 이상 쓰지 않는다.
        "ALTER TABLE missions ADD COLUMN reward_stickers INTEGER DEFAULT 1",
        # 일정 완료 승인에서 실제로 준 포인트 (거절/재요청 추적용)
        "ALTER TABLE mission_completions ADD COLUMN stickers_awarded INTEGER DEFAULT 0",
        "ALTER TABLE mission_completions ADD COLUMN reject_reason TEXT",
        # 가족 내 역할/권한. leader = 부모(승인·관리), member = 자녀.
        # 최초 생성자는 _family_role()에서 자동으로 leader 취급한다.
        "ALTER TABLE devices ADD COLUMN family_role TEXT",
        # 이 미션을 해내면 소원 하나를 고를 수 있다 (목표 미션).
        "ALTER TABLE missions ADD COLUMN grants_wish INTEGER DEFAULT 0",
        # 일정 담당자 — member_id는 기기별 로컬 UUID라 동기화해도 서로 못 알아본다.
        # 사람을 가리키는 user_id여야 아이 기기에서도 "내 일정"으로 보인다.
        "ALTER TABLE shared_events ADD COLUMN assignee_user_id TEXT",
        "ALTER TABLE shared_events ADD COLUMN location TEXT",
        # 아이가 올린 일정은 부모가 확인하기 전까지 가족 캘린더에 나가지 않는다.
        # 기존 일정은 NULL인데 조회에서 approved로 취급해 과거 일정이 숨지 않게 한다.
        "ALTER TABLE shared_events ADD COLUMN status TEXT DEFAULT 'approved'",
        "ALTER TABLE shared_events ADD COLUMN reject_reason TEXT",
        # 반복 등록으로 한 번에 만들어진 묶음 — 통째로 지울 때 쓴다.
        "ALTER TABLE shared_events ADD COLUMN series_id TEXT",
    ]:
        try:
            await DB.execute(alter)
        except Exception:
            pass  # 이미 컬럼 존재하면 무시


# ── 환경 플래그 ───────────────────────────────────────────────────────────────

_DEBUG_ROUTES_ENABLED = os.environ.get("ENABLE_DEBUG_ROUTES", "").lower() in {"1", "true", "yes"}
_ENVIRONMENT = os.environ.get("ENVIRONMENT", "development").lower()
_IS_PRODUCTION = _ENVIRONMENT == "production"
# /api/dev/reset-device를 인증하기 위한 공유 비밀 — 미설정이면 엔드포인트가 항상 거부.
_DEV_RESET_SECRET = os.environ.get("DEV_RESET_SECRET", "")


def _resolve_allowed_origins() -> List[str]:
    """ALLOWED_ORIGINS 콤마 구분 환경변수에서 화이트리스트 로드.
    prod에서 미설정이면 빈 리스트(차단), dev에서는 와일드카드."""
    raw = os.environ.get("ALLOWED_ORIGINS", "").strip()
    if raw:
        return [o.strip() for o in raw.split(",") if o.strip()]
    if _IS_PRODUCTION:
        print("⚠️  ALLOWED_ORIGINS 미설정: 프로덕션에서 모든 CORS 요청이 차단됩니다.")
        return []
    return ["*"]

# ── FastAPI 앱 ─────────────────────────────────────────────────────────────────

app = FastAPI(title="오늘우리집 서버", version="4.0.0")
app.add_middleware(
    CORSMiddleware,
    allow_origins=_resolve_allowed_origins(),
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.on_event("startup")
async def startup():
    await init_db()


# ── 인증 미들웨어 ─────────────────────────────────────────────────────────────

_NO_AUTH_PATHS = {
    "/api/ping",
    "/api/auth/social", "/api/auth/email/register", "/api/auth/email/login",
    "/api/auth/restore",  # 앱 재시작 시 device_secret 없이 사용자 정보 복원
    "/api/dev/reset-device",  # secret 본문 검증으로 자체 보호
}
if _DEBUG_ROUTES_ENABLED:
    _NO_AUTH_PATHS.update({"/api/debug/db", "/api/debug/register-test", "/api/debug/user"})


@app.middleware("http")
async def device_auth_middleware(request: Request, call_next):
    path = request.url.path
    if (path in _NO_AUTH_PATHS or path.startswith("/docs") or path.startswith("/openapi")
            or path.startswith("/join/") or path.startswith("/.well-known/")
            or path == "/learn" or path.startswith("/learn/")  # 학습 웹앱: 쿠키로 자체 인증
            or path in {"/privacy", "/terms", "/location-terms"}):  # 공개 약관 URL
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


@app.exception_handler(Exception)
async def unhandled_exception_handler(request: Request, exc: Exception):
    """예상 못 한 예외도 JSON으로 반환 — 기본 500 응답은 JSON이 아니라서
    클라이언트가 '서버 응답을 처리할 수 없어요'로만 표시되고 원인이 가려진다."""
    print(f"[ERROR] {request.method} {request.url.path} — {type(exc).__name__}: {exc}")
    return JSONResponse(
        {"detail": f"서버 오류가 발생했어요 ({type(exc).__name__})"},
        status_code=500,
    )


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
    user_id: Optional[str] = None

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

class ChatSendReq(BaseModel):
    device_id: str
    content: str


class DevResetReq(BaseModel):
    secret: str
    device_id: str


class MissionUpsertReq(BaseModel):
    device_id: str  # 호출자(부모) 인증용 — 헤더 device와 일치 검증
    title: str
    description: Optional[str] = None
    reward_points: int = 0        # 레거시 — 지급에 쓰이지 않음 (과거 기록 호환)
    reward_stickers: int = 1      # 미션 보상: 포도알 개수
    grants_wish: bool = False     # 해내면 소원 하나를 고를 수 있는 목표 미션
    assignee_device_id: Optional[str] = None
    icon: Optional[str] = None
    require_approval: bool = True
    recurrence: str = "none"  # none | daily | weekly


class MissionCompleteReq(BaseModel):
    device_id: str  # 수행자


class CompletionApproveReq(BaseModel):
    device_id: str  # 승인자(부모)
    action: str     # 'approve' / 'reject'
    reason: Optional[str] = None  # 거절 사유 — 아이가 뭘 고쳐야 할지 알 수 있게


class EventCompleteReq(BaseModel):
    device_id: str            # 완료를 누른 사람
    event_id: str
    event_title: Optional[str] = None
    points: int = 0           # 클라이언트가 계산한 포상 포인트
    target_user_id: Optional[str] = None   # 일정 담당자 (없으면 누른 사람)


class RewardUpsertReq(BaseModel):
    device_id: str
    title: str
    description: Optional[str] = None
    cost_points: int
    icon: Optional[str] = None
    stock: Optional[int] = None


class RewardRedeemReq(BaseModel):
    device_id: str  # 신청자


class RedemptionApproveReq(BaseModel):
    device_id: str  # 승인자
    action: str     # 'approve' / 'reject'


class StickerBoardConfigReq(BaseModel):
    device_id: str  # 방장(부모) 인증용
    goal_count: int
    reward_title: str
    reward_description: Optional[str] = None


class EventRewardReq(BaseModel):
    device_id: str                          # 완료 처리를 누른 사람 (인증용)
    # 포상을 받을 사람. user_id가 정답 — 기기를 바꿔도 사람은 그대로다.
    # target_device_id는 user_id를 아직 모르는 경우의 폴백.
    target_user_id: Optional[str] = None
    target_device_id: Optional[str] = None
    points: int
    event_id: str
    event_title: Optional[str] = None
    # 보상 분리 이후 일정은 포인트만 준다. 포도알은 미션 몫.
    grant_sticker: bool = False


class ListUpsertReq(BaseModel):
    device_id: str
    title: str
    type: str = "checklist"   # 'shopping' | 'todo' | 'checklist'
    icon: Optional[str] = None


class ListItemCreateReq(BaseModel):
    device_id: str
    content: str
    quantity: Optional[str] = None


class ListItemPatchReq(BaseModel):
    device_id: str
    content: Optional[str] = None
    quantity: Optional[str] = None
    is_checked: Optional[bool] = None


class EventCommentReq(BaseModel):
    device_id: str
    content: str


class EventSeenReq(BaseModel):
    device_id: str


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
    assignee_user_id: Optional[str] = None   # 담당자 계정 (기기 바뀌어도 유지)
    location: Optional[str] = None
    series_id: Optional[str] = None          # 반복 등록 묶음

# ── 헬퍼 ─────────────────────────────────────────────────────────────────────

def _now() -> str:
    return datetime.now().isoformat()


def _now_utc() -> str:
    """UTC ISO 문자열 (채팅 메시지처럼 클라이언트가 toLocal() 하는 데이터용)"""
    return datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%S.%f') + 'Z'


def _hash_pw(pw: str) -> str:
    """bcrypt 해시 ($2b$로 시작, 60자). 신규 가입/재해싱에서 사용."""
    return bcrypt.hashpw(pw.encode("utf-8"), bcrypt.gensalt()).decode("ascii")


def _verify_pw(pw: str, stored: Optional[str]) -> Tuple[bool, bool]:
    """비밀번호 검증. 반환: (verified, needs_rehash)
    - bcrypt(raw) 매치 → (True, False)  # 이상적 상태
    - bcrypt(sha256_hex) 매치 → (True, True)  # 마이그레이션 wrap, raw로 업그레이드 권장
    - 레거시 sha256_hex 직접 매치 → (True, True)  # 미마이그레이션
    - 불일치 → (False, False)
    """
    if not stored:
        return False, False
    if stored.startswith("$2"):
        try:
            if bcrypt.checkpw(pw.encode("utf-8"), stored.encode("ascii")):
                return True, False
            sha_hex = hashlib.sha256(pw.encode("utf-8")).hexdigest()
            if bcrypt.checkpw(sha_hex.encode("ascii"), stored.encode("ascii")):
                return True, True
        except Exception:
            pass
        return False, False
    if len(stored) == 64:
        legacy = hashlib.sha256(pw.encode("utf-8")).hexdigest()
        if legacy == stored:
            return True, True
    return False, False


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


async def _inherit_previous_device(user_id: str, new_device_id: str) -> Optional[str]:
    """기기를 바꾼 사용자의 자산을 새 기기로 옮긴다.

    device_id는 기기마다 새로 만들어지는데 포인트·스티커·미션 기록이 전부 그 키에
    묶여 있어서, 이관하지 않으면 폰을 바꾼 순간 모아둔 게 전부 사라진다.
    아이 입장에서 포도송이가 없어지는 게 제일 치명적이라 로그인 시점에 승계한다.

    반환값: 승계한 옛 기기 id (없으면 None)
    """
    old = await DB.fetchrow(
        "SELECT d.id, d.family_id FROM devices d "
        "JOIN device_secrets ds ON ds.device_id = d.id "
        "WHERE ds.user_id = ? AND d.id != ? AND d.family_id IS NOT NULL "
        "ORDER BY ds.created_at DESC LIMIT 1",
        user_id, new_device_id,
    )
    if not old:
        return None

    old_id, family_id = old["id"], old["family_id"]

    # 1) 가족 소속 승계 — 초대코드로 다시 들어가지 않아도 되게.
    await DB.execute("UPDATE devices SET family_id = ? WHERE id = ?", family_id, new_device_id)

    # 2) 포인트 잔액 합산 — 새 기기에 이미 행이 있을 수 있어 더한 뒤 옛 행을 지운다.
    bal = await DB.fetchrow(
        "SELECT balance FROM family_points WHERE family_id = ? AND device_id = ?",
        family_id, old_id,
    )
    if bal and int(bal["balance"]) != 0:
        await _adjust_balance(family_id, new_device_id, int(bal["balance"]))
    await DB.execute(
        "DELETE FROM family_points WHERE family_id = ? AND device_id = ?", family_id, old_id
    )

    # 3) 기기 id를 키로 쓰는 기록들을 전부 새 기기로 이전.
    for sql in (
        "UPDATE member_stickers SET device_id = ? WHERE device_id = ?",
        "UPDATE sticker_board_completions SET device_id = ? WHERE device_id = ?",
        "UPDATE mission_completions SET user_device_id = ? WHERE user_device_id = ?",
        "UPDATE reward_redemptions SET user_device_id = ? WHERE user_device_id = ?",
        "UPDATE missions SET assignee_device_id = ? WHERE assignee_device_id = ?",
        "UPDATE missions SET created_by = ? WHERE created_by = ?",
        "UPDATE shared_events SET created_by = ? WHERE created_by = ?",
        "UPDATE chat_messages SET device_id = ? WHERE device_id = ?",
        "UPDATE event_comments SET device_id = ? WHERE device_id = ?",
        # 새 기기도 이미 본 일정이면 PK가 겹치므로, 옛 기기 기록을 먼저 지우고 옮긴다.
    ):
        try:
            await DB.execute(sql, new_device_id, old_id)
        except Exception as e:
            # 컬럼 구성이 다른 배포본에서도 나머지 이관은 계속돼야 한다.
            print(f"[inherit] 건너뜀: {sql.split(' SET ')[0]} — {e}")

    # 3-1) 일정 확인 기록 — (event_id, device_id)가 PK라 새 기기도 이미 본 일정이면
    #      그대로 옮기면 충돌한다. 겹치는 옛 기록만 지우고 나머지를 옮긴다.
    try:
        await DB.execute(
            "DELETE FROM event_views WHERE device_id = ? AND event_id IN "
            "(SELECT event_id FROM event_views WHERE device_id = ?)",
            old_id, new_device_id,
        )
        await DB.execute(
            "UPDATE event_views SET device_id = ? WHERE device_id = ?", new_device_id, old_id
        )
    except Exception as e:
        print(f"[inherit] 건너뜀: event_views — {e}")

    # 4) 방장이었다면 방장 자리도 승계 (아니면 가족 관리가 잠긴다).
    await DB.execute(
        "UPDATE families SET created_by = ? WHERE id = ? AND created_by = ?",
        new_device_id, family_id, old_id,
    )

    # 5) 전화번호도 옮긴다 — 연락처로 이 사람을 찾을 수 있어야 한다.
    try:
        await DB.execute("DELETE FROM device_phones WHERE device_id = ?", new_device_id)
        await DB.execute(
            "UPDATE device_phones SET device_id = ? WHERE device_id = ?", new_device_id, old_id
        )
    except Exception as e:
        print(f"[inherit] 전화번호 이관 건너뜀 — {e}")

    # 6) 옛 기기는 가족에서 뗀다. 같은 사람이 두 명으로 보이면 안 된다.
    await DB.execute("UPDATE devices SET family_id = NULL WHERE id = ?", old_id)

    print(f"[inherit] {old_id} → {new_device_id} (user={user_id}, family={family_id})")
    return old_id


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
    # user_id를 함께 싣는다 — 기기는 바뀌어도 사람은 그대로라, 포상 대상 지정은
    # device_id가 아니라 user_id로 해야 기기 변경 후에도 연결이 살아남는다.
    members_raw = await DB.fetch(
        "SELECT d.id, d.user_name, d.family_role, ds.user_id "
        "FROM devices d LEFT JOIN device_secrets ds ON ds.device_id = d.id "
        "WHERE d.family_id = ?",
        family_id,
    )
    members = [
        {
            "device_id": m["id"],
            "user_id": m["user_id"],
            "user_name": m["user_name"],
            "is_creator": m["id"] == family["created_by"],
            # 대표는 created_by 한 명. 예전 'leader' 값은 parent로 읽는다.
            "family_role": "leader" if m["id"] == family["created_by"]
                           else ("parent" if m["family_role"] == "leader"
                                 else (m["family_role"] or "member")),
        }
        for m in members_raw
    ]
    return family, members

# ── 엔드포인트 ────────────────────────────────────────────────────────────────

@app.get("/api/ping")
async def ping():
    return {"status": "ok", "message": "오늘우리집 서버 정상 동작 중", "build": "v4-fix-16"}


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


if _DEBUG_ROUTES_ENABLED:
    app.add_api_route("/api/debug/db", debug_db, methods=["GET"], include_in_schema=False)
    app.add_api_route("/api/debug/user", debug_user, methods=["GET"], include_in_schema=False)
    app.add_api_route("/api/debug/register-test", debug_register_test, methods=["POST"], include_in_schema=False)


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
        # Apple은 재로그인 시 name을 빈 문자열로 보냄 → NULLIF로 빈 값 무시, 기존 이름 유지
        await DB.execute(
            "UPDATE users SET name = COALESCE(NULLIF(?, ''), name), email = COALESCE(?, email), profile_image_url = COALESCE(?, profile_image_url) WHERE id = ?",
            data.name, data.email, data.profile_image_url, user_id,
        )

    device_secret = str(uuid.uuid4())
    await _upsert_device_secret(data.device_id, device_secret, user_id)
    await _upsert_device(data.device_id, data.name)
    inherited = await _inherit_previous_device(user_id, data.device_id)

    return {
        "device_secret": device_secret,
        "user_id": user_id,
        "inherited_from": inherited,
    }


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
    inherited = await _inherit_previous_device(user_id, data.device_id)

    return {
        "user_id": user_id,
        "token": device_secret,
        "name": data.name,
        "inherited_from": inherited,
    }


@app.post("/api/auth/email/login")
async def email_login(data: EmailLoginReq):
    """이메일 로그인 — 레거시 SHA-256 해시도 검증하고 bcrypt로 자동 업그레이드."""
    user = await DB.fetchrow(
        "SELECT * FROM users WHERE email = ? AND (provider IS NULL OR provider = 'email')",
        data.email,
    )
    verified, needs_rehash = (False, False)
    if user:
        verified, needs_rehash = _verify_pw(data.password, user.get("password_hash"))
    if not user or not verified:
        raise HTTPException(status_code=401, detail="이메일 또는 비밀번호가 일치하지 않아요")

    # 레거시/wrap 해시면 raw bcrypt로 교체
    if needs_rehash:
        await DB.execute(
            "UPDATE users SET password_hash = ? WHERE id = ?",
            _hash_pw(data.password), user["id"],
        )

    device_secret = str(uuid.uuid4())
    await _upsert_device_secret(data.device_id, device_secret, user["id"])
    await _upsert_device(data.device_id, user["name"])
    inherited = await _inherit_previous_device(user["id"], data.device_id)

    return {
        "user_id": user["id"],
        "token": device_secret,
        "name": user["name"],
        "inherited_from": inherited,
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
    # phone_number 에는 UNIQUE 제약이 있어서, 같은 번호가 다른 기기에 남아 있으면
    # ON CONFLICT(device_id) 로는 못 잡고 UniqueViolation(500)이 난다.
    # 앱 재설치/기기 변경 시 device_id 가 새로 생기므로 번호 소유권을 최신 기기로 이전한다.
    await DB.execute(
        "DELETE FROM device_phones WHERE phone_number = ? AND device_id != ?",
        data.phone_number, data.device_id,
    )
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
async def create_invite_code(request: Request, device_id: str):
    # 헤더로 인증된 device_id와 쿼리의 device_id가 일치해야 한다 (남의 가족 invite 발급 방지).
    auth_device = request.headers.get("X-Device-ID", "")
    if auth_device and auth_device != device_id:
        raise HTTPException(status_code=403, detail="다른 기기의 초대 코드를 발급할 수 없어요")
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
    """초대 코드 검증 + 즉시 자동 합류 (방장 승인 절차 제거).

    invite_code는 1회용이라 사실상 인증된 코드임 — 별도 승인은 UX 부담만 증가.
    어린이/노인도 단순히 코드/QR 입력만으로 가족이 됨. 방장은 알림으로 합류 사실만 받음.
    """
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

    family_id = invite["family_id"]

    # 1) device 생성/업데이트 + family_id 즉시 부여
    if not device:
        if _USE_PG:
            await DB.execute(
                "INSERT INTO devices (id, user_name, family_id, created_at) VALUES ($1, $2, $3, NOW())",
                data.device_id, data.device_name, family_id,
            )
        else:
            await DB.execute(
                "INSERT INTO devices (id, user_name, family_id, created_at) VALUES (?, ?, ?, ?)",
                data.device_id, data.device_name, family_id, _now(),
            )
    else:
        await DB.execute(
            "UPDATE devices SET user_name = ?, family_id = ? WHERE id = ?",
            data.device_name, family_id, data.device_id,
        )

    # 2) 초대 코드 1회용 처리
    if _USE_PG:
        await DB.execute("UPDATE invite_codes SET used = TRUE WHERE code = ?", data.code)
    else:
        await DB.execute("UPDATE invite_codes SET used = 1 WHERE code = ?", data.code)

    # 3) join_requests를 'confirmed'로 기록 — 방장이 알림으로 보고 새 멤버 확인용
    request_id = str(uuid.uuid4())
    if _USE_PG:
        await DB.execute(
            "INSERT INTO join_requests (id, family_id, device_id, device_name, status, created_at)"
            " VALUES ($1, $2, $3, $4, 'confirmed', NOW())",
            request_id, family_id, data.device_id, data.device_name,
        )
    else:
        await DB.execute(
            "INSERT INTO join_requests (id, family_id, device_id, device_name, status, created_at)"
            " VALUES (?, ?, ?, ?, 'confirmed', ?)",
            request_id, family_id, data.device_id, data.device_name, _now(),
        )

    # 가족 이름도 같이 반환 → 클라이언트가 즉시 UI 갱신
    family = await DB.fetchrow("SELECT name FROM families WHERE id = ?", family_id)
    return {
        "request_id": request_id,
        "status": "confirmed",
        "family_id": family_id,
        "family_name": family["name"] if family else None,
    }


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
        "SELECT id, device_id, device_name AS requester_name, status, created_at"
        " FROM join_requests WHERE family_id = ? AND status = 'pending'",
        fid,
    )
    def _serialize_row(r: dict) -> dict:
        return {k: (v.isoformat() if hasattr(v, 'isoformat') else v) for k, v in r.items()}
    return {"requests": [_serialize_row(r) for r in rows]}


@app.get("/api/family/join-status")
async def get_join_status(request_id: str):
    row = await DB.fetchrow(
        "SELECT status, family_id FROM join_requests WHERE id = ?", request_id
    )
    if not row:
        raise HTTPException(status_code=404, detail="요청을 찾을 수 없어요")
    return {"status": row["status"], "family_id": row["family_id"]}


_PLAY_STORE_URL = "https://play.google.com/store/apps/details?id=com.familyhub.today_our_home"
_IOS_TEAM_ID = "DS988F8YKR"
_IOS_BUNDLE_ID = "com.familyhub.todayOurHome"
_ANDROID_PACKAGE = "com.familyhub.today_our_home"
_ANDROID_CERT_SHA256 = "CF:3A:03:B4:58:A9:73:FF:1E:43:B3:60:9D:4B:FC:ED:08:73:6A:E8:F1:F5:63:0B:E4:CD:09:9A:8B:89:D1:A4"


@app.get("/.well-known/assetlinks.json")
async def android_assetlinks():
    return JSONResponse([{
        "relation": ["delegate_permission/common.handle_all_urls"],
        "target": {
            "namespace": "android_app",
            "package_name": _ANDROID_PACKAGE,
            "sha256_cert_fingerprints": [_ANDROID_CERT_SHA256],
        },
    }])


@app.get("/.well-known/apple-app-site-association")
async def ios_aasa():
    return JSONResponse({
        "applinks": {
            "apps": [],
            "details": [{"appID": f"{_IOS_TEAM_ID}.{_IOS_BUNDLE_ID}", "paths": ["/join/*"]}],
        }
    })


@app.get("/join/{code}", response_class=HTMLResponse)
async def join_landing_page(code: str):
    """QR/링크로 초대 코드에 접근했을 때의 웹 폴백 페이지.
    App Links/Universal Links가 잡으면 이 페이지 대신 앱이 바로 열린다."""
    family_name = None
    try:
        row = await DB.fetchrow(
            "SELECT f.name FROM invite_codes ic JOIN families f ON f.id = ic.family_id "
            "WHERE ic.code = ? AND ic.used = 0 AND ic.expires_at > ?",
            code, _now(),
        )
        if row:
            family_name = row["name"]
    except Exception:
        pass

    heading = f"'{family_name}' 가족에 초대됐어요" if family_name else "가족 초대 코드"
    notice = "" if family_name else (
        '<p class="warn">코드가 만료됐거나 이미 사용됐을 수 있어요. '
        '가족에게 새 코드를 요청해주세요.</p>'
    )
    html = f"""<!DOCTYPE html>
<html lang="ko">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>오늘우리집 가족 초대</title>
<style>
  body {{ font-family: -apple-system, BlinkMacSystemFont, 'Pretendard', sans-serif;
         background: #FFF7F5; margin: 0; padding: 24px; display: flex;
         min-height: 100vh; align-items: center; justify-content: center; }}
  .card {{ background: #fff; border-radius: 24px; padding: 36px 28px; max-width: 360px;
          width: 100%; text-align: center; box-shadow: 0 8px 24px rgba(0,0,0,0.08); }}
  .emoji {{ font-size: 48px; }}
  h1 {{ font-size: 19px; color: #26221F; margin: 12px 0 8px; }}
  p {{ font-size: 13px; color: #8A8580; line-height: 1.6; }}
  .warn {{ color: #E4574C; font-weight: 600; }}
  .code {{ font-size: 32px; font-weight: 900; letter-spacing: 6px; color: #FF6B8A;
          background: #FFF0EE; border-radius: 14px; padding: 16px; margin: 20px 0; }}
  button {{ width: 100%; padding: 14px; border: none; border-radius: 14px;
           background: #FF6B8A; color: #fff; font-size: 15px; font-weight: 800;
           margin-bottom: 10px; }}
  a.store {{ display: block; padding: 12px; border-radius: 14px; border: 1.5px solid #FF6B8A;
            color: #FF6B8A; text-decoration: none; font-weight: 700; font-size: 14px; }}
</style>
</head>
<body>
  <div class="card">
    <div class="emoji">🏡</div>
    <h1>{heading}</h1>
    <p>오늘우리집 앱이 설치되어 있으면 자동으로 열려요.<br>안 열리면 앱에서 아래 코드를 직접 입력해주세요.</p>
    {notice}
    <div class="code" id="code">{code}</div>
    <button onclick="navigator.clipboard.writeText('{code}');this.textContent='복사됐어요 ✓'">코드 복사</button>
    <a class="store" href="{_PLAY_STORE_URL}">Android 앱 설치</a>
  </div>
</body>
</html>"""
    return HTMLResponse(html)


_PRIVACY_CONTACT = "zzz0903apple@gmail.com"
_PRIVACY_UPDATED = "2026-09-08"


@app.get("/privacy", response_class=HTMLResponse)
async def privacy_policy_page():
    """공개 개인정보처리방침. Play Console 등록에 필요한 URL이라 인증 없이 열려야 한다.

    내용은 실제 저장 항목(위 CREATE TABLE들)과 AndroidManifest 권한에 맞춰 유지할 것 —
    Play의 데이터 안전(Data safety) 양식과 어긋나면 심사에서 반려된다."""
    html = f"""<!DOCTYPE html>
<html lang="ko">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>오늘우리집 개인정보처리방침</title>
<style>
  body {{ font-family: -apple-system, BlinkMacSystemFont, 'Pretendard', sans-serif;
         background: #FFF7F5; margin: 0; padding: 20px; color: #26221F; }}
  .wrap {{ background: #fff; border-radius: 20px; padding: 28px 22px; max-width: 720px;
          margin: 0 auto; box-shadow: 0 8px 24px rgba(0,0,0,0.06); line-height: 1.7; }}
  h1 {{ font-size: 22px; margin: 0 0 4px; }}
  h2 {{ font-size: 16px; margin: 28px 0 8px; color: #FF6B8A; }}
  .meta {{ font-size: 12px; color: #8A8580; margin-bottom: 20px; }}
  p, li {{ font-size: 14px; color: #4A4540; }}
  ul {{ padding-left: 20px; }}
  table {{ width: 100%; border-collapse: collapse; margin: 10px 0; font-size: 13px; }}
  th, td {{ border: 1px solid #EFE7E5; padding: 8px 10px; text-align: left; vertical-align: top; }}
  th {{ background: #FFF0EE; color: #26221F; font-weight: 700; }}
  .note {{ background: #FFF8E1; border-radius: 12px; padding: 12px 14px; font-size: 13px;
          color: #7A6A3A; margin: 14px 0; }}
  a {{ color: #FF6B8A; }}
</style>
</head>
<body>
<div class="wrap">
  <h1>오늘우리집 개인정보처리방침</h1>
  <div class="meta">최종 수정일: {_PRIVACY_UPDATED}</div>

  <p>오늘우리집(이하 "서비스")은 「개인정보 보호법」을 준수하며, 가족이 일정·준비물·집안일을
  함께 관리하는 데 필요한 최소한의 정보만 수집합니다.</p>

  <h2>1. 수집하는 정보와 이용 목적</h2>
  <table>
    <tr><th>항목</th><th>목적</th><th>보관 위치</th></tr>
    <tr><td>이메일, 이름</td><td>회원 식별·로그인(카카오·구글·애플·네이버 또는 이메일 가입)</td><td>서버</td></tr>
    <tr><td>기기 식별자</td><td>가족 그룹 동기화 및 기기 인증</td><td>서버</td></tr>
    <tr><td>전화번호(선택)</td><td>연락처 기반 가족 찾기·초대</td><td>서버</td></tr>
    <tr><td>가족 공유 데이터<br>(일정·코멘트·리스트·식단·미션·포인트·스티커·채팅)</td>
        <td>가족 구성원 간 실시간 공유</td><td>서버</td></tr>
    <tr><td>비공개 일정·준비물·문서 스캔 결과</td><td>본인만 확인</td><td>기기 내부에만 저장</td></tr>
    <tr><td><b>위치정보</b>(선택)<br>안전 구역 도착·출발 기록, 요청 시 1회 좌표</td>
        <td>가족 안전 확인 — 별도 동의한 경우에만</td>
        <td>서버<br>(이동 기록 30일 후 자동 삭제)</td></tr>
  </table>
  <div class="note">위치정보는 <b>선택 동의</b> 항목입니다. 동의하지 않아도 나머지 기능은 모두
  사용할 수 있으며, 동의를 해제하면 그동안 쌓인 이동 기록과 조회 이력이 <b>즉시 삭제</b>됩니다.
  자세한 내용은 <a href="/location-terms">위치기반서비스 이용약관</a>을 참고해주세요.</div>

  <h2>2. 앱 권한과 사용 이유</h2>
  <ul>
    <li><b>알림 접근</b> — 학교·유치원 알림(하이클래스, 키즈노트 등)에서 일정을 자동으로 뽑아냅니다.
        알림 내용은 기기에서 처리되며, 사용자가 일정으로 저장하기 전까지 서버로 전송되지 않습니다.</li>
    <li><b>연락처</b> — 이미 서비스를 쓰는 가족을 찾기 위해, 선택한 연락처의 전화번호를
        서버에 조회용으로 보냅니다. 조회 결과만 돌려받으며 <b>보낸 번호는 저장하지 않습니다.</b>
        이름·이메일 등 나머지 연락처 정보는 기기를 벗어나지 않습니다.</li>
    <li><b>캘린더</b> — 기기 캘린더와 일정을 주고받습니다.</li>
    <li><b>카메라·사진</b> — 가정통신문을 촬영해 글자를 인식(OCR)합니다. 인식은 기기에서 처리됩니다.</li>
    <li><b>위치(선택)</b> — 가족이 등록한 안전 구역에 도착·출발할 때, 그리고 가족이
        위치를 물었을 때만 사용합니다. <b>앱 사용 중에만 확인하며 백그라운드 상시 추적을
        하지 않습니다.</b>
        누군가 내 위치를 조회하면 <b>본인에게 알림이 갑니다.</b></li>
    <li><b>건강 데이터(선택)</b> — 사용자가 직접 연결한 경우에만 읽습니다.
        기기 안에서만 쓰이며 <b>서버로 전송되지 않습니다.</b></li>
    <li><b>이메일 연결(선택)</b> — 사용자가 등록한 계정의 가정통신문 메일을 분류합니다.</li>
  </ul>
  <div class="note">민감정보(건강·의료)는 기본적으로 비공개로 처리되며, 사용자가 명시적으로
  공유를 선택하기 전에는 다른 가족 구성원에게도 보이지 않습니다.</div>

  <h2>3. 제3자 제공 및 광고</h2>
  <p>서비스는 수집한 개인정보를 <b>제3자에게 판매하거나 제공하지 않으며, 광고 목적으로 이용하지 않습니다.</b>
  로그인 제공자(카카오·구글·애플·네이버)에는 인증에 필요한 최소 정보만 오갑니다.</p>

  <h2>4. 보유 및 파기</h2>
  <ul>
    <li>회원 탈퇴 또는 가족 그룹 삭제 시 관련 데이터를 지체 없이 삭제합니다.</li>
    <li>앱 내 <b>설정 → 모든 데이터 삭제</b>로 언제든 직접 삭제할 수 있습니다.</li>
    <li>기기에만 저장된 정보는 앱 삭제 시 함께 사라집니다.</li>
    <li><b>위치 이동 기록은 30일</b>만 보관하고 자동 삭제합니다.
        위치 공유를 끄면 즉시 전부 삭제됩니다.</li>
  </ul>

  <h2>5. 아동의 개인정보</h2>
  <p>만 14세 미만 아동의 정보는 법정대리인(보호자)이 가족 그룹을 만들고 동의한 범위에서만 처리합니다.</p>

  <h2>6. 이용자의 권리</h2>
  <p>이용자는 언제든지 자신의 개인정보에 대한 열람·정정·삭제·처리정지를 요구할 수 있습니다.
  앱 내 설정 메뉴 또는 아래 연락처로 요청해주세요.</p>

  <h2>7. 관련 약관</h2>
  <p><a href="/terms">서비스 이용약관</a> · <a href="/location-terms">위치기반서비스 이용약관</a></p>

  <h2>8. 문의</h2>
  <p>개인정보 보호책임자: 오늘우리집 운영자<br>
  이메일: <a href="mailto:{_PRIVACY_CONTACT}">{_PRIVACY_CONTACT}</a></p>
</div>
</body>
</html>"""
    return HTMLResponse(html)


_LEGAL_CSS = """
  body { font-family: -apple-system, BlinkMacSystemFont, 'Pretendard', sans-serif;
         background: #FFF7F5; margin: 0; padding: 20px; color: #26221F; }
  .wrap { background: #fff; border-radius: 20px; padding: 28px 22px; max-width: 720px;
          margin: 0 auto; box-shadow: 0 8px 24px rgba(0,0,0,0.06); line-height: 1.7; }
  h1 { font-size: 22px; margin: 0 0 4px; }
  h2 { font-size: 16px; margin: 26px 0 8px; color: #FF6B8A; }
  .meta { font-size: 12px; color: #8A8580; margin-bottom: 20px; }
  p, li { font-size: 14px; color: #4A4540; }
  ul { padding-left: 20px; }
  .note { background: #FFF8E1; border-radius: 12px; padding: 12px 14px; font-size: 13px;
          color: #7A6A3A; margin: 14px 0; }
  a { color: #FF6B8A; }
"""


def _legal_page(title: str, body: str) -> HTMLResponse:
    return HTMLResponse(f"""<!DOCTYPE html>
<html lang="ko"><head><meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>{title}</title><style>{_LEGAL_CSS}</style></head>
<body><div class="wrap"><h1>{title}</h1>
<div class="meta">최종 수정일: {_PRIVACY_UPDATED}</div>
{body}
<h2>문의</h2><p><a href="mailto:{_PRIVACY_CONTACT}">{_PRIVACY_CONTACT}</a></p>
</div></body></html>""")


@app.get("/terms", response_class=HTMLResponse)
async def terms_page():
    return _legal_page("오늘우리집 서비스 이용약관", """
  <h2>제1조 (목적)</h2>
  <p>이 약관은 오늘우리집(이하 "서비스")이 제공하는 가족 일정·집안일 관리 서비스의
  이용 조건과 절차를 정합니다.</p>

  <h2>제2조 (서비스 내용)</h2>
  <ul>
    <li>가족 간 일정·준비물·리스트·식단 공유</li>
    <li>미션과 포도알·포인트 보상</li>
    <li>학교 알림·가정통신문에서 일정 자동 추출 (Android)</li>
    <li>가족 위치 공유 및 안전 구역 알림 (선택, 별도 동의 필요)</li>
  </ul>

  <h2>제3조 (계정과 가족 그룹)</h2>
  <p>가족 그룹을 만든 사람이 방장이 되며, 구성원 관리·미션 승인 권한을 갖습니다.
  가족 그룹을 나가면 그 가족의 공유 데이터에 더 이상 접근할 수 없습니다.</p>

  <h2>제4조 (아동의 이용)</h2>
  <p>만 14세 미만 아동은 법정대리인(보호자)이 가족 그룹을 만들고 동의한 범위에서만
  서비스를 이용할 수 있습니다.</p>

  <h2>제5조 (이용자의 의무)</h2>
  <ul>
    <li>타인의 정보를 도용하거나 허위로 등록하지 않습니다.</li>
    <li>가족 구성원의 동의 없이 위치 등 개인정보를 수집·이용하지 않습니다.</li>
  </ul>

  <h2>제6조 (서비스 변경·중단)</h2>
  <p>서비스는 기능을 개선하거나 중단할 수 있으며, 중요한 변경은 앱 내에서 미리 알립니다.</p>

  <h2>제7조 (면책)</h2>
  <p>서비스는 가족 간 정보 공유를 돕는 도구이며, 위치 정보의 정확성이나 알림의 도달을
  보증하지 않습니다. 긴급 상황에서는 반드시 직접 연락하거나 공적 기관을 이용해주세요.</p>
""")


@app.get("/location-terms", response_class=HTMLResponse)
async def location_terms_page():
    return _legal_page("위치기반서비스 이용약관", """
  <div class="note">「위치정보의 보호 및 이용 등에 관한 법률」에 따라, 위치 기능은
  <b>선택 동의</b> 항목입니다. 동의하지 않아도 나머지 기능은 모두 사용할 수 있습니다.</div>

  <h2>제1조 (목적)</h2>
  <p>이 약관은 오늘우리집이 제공하는 위치기반서비스의 이용 조건을 정합니다.</p>

  <h2>제2조 (수집하는 위치정보와 이용 목적)</h2>
  <ul>
    <li><b>안전 구역 도착·출발 기록</b> — 집·학교·학원 등 가족이 직접 등록한 장소에
        도착하거나 떠날 때의 시각과 장소명.
        <b>앱이 실행 중일 때만</b> 확인하며, 백그라운드에서 상시 추적하지 않습니다.</li>
    <li><b>요청 시 현재 위치</b> — 가족이 "지금 어디야?"를 요청하고 본인 기기가
        응답할 때의 1회 좌표</li>
  </ul>
  <p>수집한 위치정보는 <b>같은 가족 그룹 구성원에게만</b> 보여지며,
  제3자에게 제공하거나 광고에 이용하지 않습니다.</p>

  <h2>제3조 (보유 기간)</h2>
  <p>이동 기록은 <b>최근 30일</b>만 보관하고 자동 삭제합니다.
  현재 위치 응답은 확인 후 24시간 내 삭제합니다.</p>

  <h2>제4조 (이용자의 권리)</h2>
  <ul>
    <li>언제든지 앱에서 <b>위치 공유를 일시정지하거나 완전히 해제</b>할 수 있습니다.</li>
    <li>누가 내 위치를 볼 수 있는지 앱에서 확인할 수 있습니다.</li>
    <li>가족이 내 위치를 조회하면 <b>나에게 알림이 갑니다.</b> 몰래 볼 수 없습니다.</li>
    <li>수집된 위치 기록의 열람·삭제를 언제든 요구할 수 있습니다.</li>
  </ul>

  <h2>제5조 (8세 이하 아동 등의 보호)</h2>
  <p>8세 이하 아동, 피성년후견인 등의 위치정보는 보호의무자의 동의가 있는 경우에만
  처리하며, 본인 동의가 있는 것으로 봅니다.</p>

  <h2>제6조 (위치정보 관리책임자)</h2>
  <p>오늘우리집 운영자 — 아래 이메일로 문의해주세요.</p>
""")


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
    # 소속은 **서버가 유일한 기준**이다. 예전엔 쿼리로 온 family_id를 그대로 믿었는데
    # (`family_id or device["family_id"]`), 그 탓에 두 가지가 터졌다.
    #   1) 남의 family_id만 알면 그 가족 구성원 명단을 그대로 받아볼 수 있었다.
    #   2) 서버에서 가족이 빠진 기기가 로컬에 남은 옛 family_id를 계속 보내면
    #      이 API만 200을 돌려줘서 앱은 "가족 있음" 화면을 그리는데, 정작
    #      _require_family를 쓰는 기능 API는 전부 400이 나는 유령 상태에 갇혔다.
    #      (초대 코드 발급·연락처 초대가 조용히 실패하던 원인)
    # family_id 인자는 구버전 앱 호환으로 받기만 하고 쓰지 않는다.
    fid = device["family_id"]
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
    await DB.execute("DELETE FROM event_views WHERE family_id = ?", family_id)
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
    # 승인된 일정 + 내가 올린 것(대기·거절 포함).
    # 남이 올린 대기 일정을 여기서 같이 주면 승인 전에 온 가족 캘린더에 떠버린다.
    # 부모가 검토할 목록은 /api/family/events/proposals로 따로 받는다.
    # status가 NULL인 과거 일정도 승인된 것으로 취급한다.
    where = "family_id = ? AND (COALESCE(status, 'approved') != 'pending' OR created_by = ?)"
    args = [device["family_id"], device_id]
    if since:
        where += " AND updated_at > ?"
        args.append(since)
    rows = await DB.fetch(
        f"SELECT * FROM shared_events WHERE {where} ORDER BY start_time", *args)
    return {"events": rows}


@app.get("/api/family/events/proposals")
async def list_event_proposals(device_id: str):
    """부모가 검토해야 할 일정 등록 요청 — 아이가 올린 것들."""
    device = await _get_device(device_id)
    family_id = await _require_family(device)
    if not await _can_manage(device_id, family_id):
        return {"proposals": []}
    rows = await DB.fetch(
        "SELECT * FROM shared_events WHERE family_id = ? AND status = 'pending' "
        "ORDER BY start_time LIMIT 100",
        family_id,
    )
    # 누가 올렸는지 보여줘야 부모가 판단할 수 있다.
    names = {
        d["id"]: d["user_name"]
        for d in await DB.fetch("SELECT id, user_name FROM devices WHERE family_id = ?", family_id)
    }
    return {
        "proposals": [
            {**dict(r), "created_by_name": names.get(r["created_by"], "가족")} for r in rows
        ]
    }


@app.post("/api/family/events/proposals/{event_id}/answer")
async def answer_event_proposal(event_id: str, data: CompletionApproveReq, request: Request):
    """일정 등록 요청 승인/거절. 거절하면 아이 기기에서도 사라진다."""
    auth = request.headers.get("X-Device-ID", "")
    if auth != data.device_id:
        raise HTTPException(status_code=403, detail="본인 기기로만 가능")
    row = await DB.fetchrow("SELECT * FROM shared_events WHERE id = ?", event_id)
    if not row:
        raise HTTPException(status_code=404, detail="일정을 찾을 수 없어요")
    if (row["status"] or "approved") != "pending":
        raise HTTPException(status_code=400, detail="이미 처리된 요청이에요")
    await _require_admin(data.device_id, row["family_id"])

    status = "approved" if data.action == "approve" else "rejected"
    await DB.execute(
        "UPDATE shared_events SET status = ?, reject_reason = ?, updated_at = ? WHERE id = ?",
        status, None if status == "approved" else data.reason, _now(), event_id,
    )
    await _chat_manager.broadcast(row["family_id"], {
        "type": "event_proposal",
        "status": status,
        "event_id": event_id,
        "title": row["title"],
        "target_device_id": row["created_by"],
        "reason": data.reason,
    })
    return {"status": status}


@app.post("/api/family/events")
async def upsert_shared_event(event: SharedEventReq, request: Request):
    existing = await DB.fetchrow(
        "SELECT id, is_completed, status FROM shared_events WHERE id = ?", event.id)
    was_completed = bool(existing and int(existing["is_completed"] or 0) == 1)
    if existing:
        # 이미 있는 일정의 승인 상태는 건드리지 않는다. 여기서 다시 계산하면
        # 아이가 자기 대기 일정을 한 번 더 올려 승인 없이 통과시킬 수 있다.
        await DB.execute("""
            UPDATE shared_events SET
                title=?, description=?, start_time=?, end_time=?,
                category=?, preparations=?, needs_reply=?, is_completed=?,
                assignee_user_id=?, location=?, series_id=?, updated_at=?
            WHERE id=?
        """, event.title, event.description, event.start_time, event.end_time,
            event.category, event.preparations, event.needs_reply, event.is_completed,
            event.assignee_user_id, event.location, event.series_id,
            event.updated_at, event.id)
    else:
        # 아이가 올린 일정은 부모 확인 대기. 부모(대표·부모)가 올리면 바로 승인.
        can_manage = await _can_manage(event.created_by, event.family_id)
        status = "approved" if can_manage else "pending"
        await DB.execute("""
            INSERT INTO shared_events
            (id, family_id, created_by, title, description, start_time, end_time,
             category, preparations, needs_reply, is_completed, updated_at,
             assignee_user_id, location, status, series_id)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, event.id, event.family_id, event.created_by, event.title, event.description,
            event.start_time, event.end_time, event.category, event.preparations,
            event.needs_reply, event.is_completed, event.updated_at,
            event.assignee_user_id, event.location, status, event.series_id)
        if status == "pending":
            await _chat_manager.broadcast(event.family_id, {
                "type": "event_proposal",
                "status": "pending",
                "event_id": event.id,
                "title": event.title,
            })
            return {"status": "pending"}

    # 방금 완료로 전환됐고 그 날 일정이 전부 끝났으면 완주 보너스 포인트
    if event.is_completed and not was_completed:
        actor = request.headers.get("X-Device-ID", "")
        if actor:
            bonus = await _maybe_grant_daily_event_bonus(
                event.family_id, actor, event.start_time[:10])
            if bonus:
                await _chat_manager.broadcast(event.family_id, {
                    "type": "daily_all_done",
                    "target_device_id": actor,
                    "points": bonus,
                })

    return {"status": "ok"}


@app.delete("/api/family/events/{event_id}")
async def delete_shared_event(event_id: str):
    await DB.execute("DELETE FROM shared_events WHERE id = ?", event_id)
    await DB.execute("DELETE FROM event_views WHERE event_id = ?", event_id)
    return {"status": "ok"}


# ── 개발용 기기 초기화 ─────────────────────────────────────────────────────────

@app.post("/api/dev/reset-device")
async def dev_reset_device(data: DevResetReq):
    """단일 기기의 가족 멤버십·세션·전화번호를 서버에서 제거.
    DEV_RESET_SECRET 환경변수가 설정되어 있어야 하며, 본문 secret이 일치해야 동작."""
    if not _DEV_RESET_SECRET:
        raise HTTPException(status_code=503, detail="기기 초기화 기능이 비활성화되어 있어요")
    if data.secret != _DEV_RESET_SECRET:
        raise HTTPException(status_code=403, detail="시크릿이 일치하지 않아요")

    # 해당 기기의 세션·전화번호·가족 소속만 해제 (다른 사용자 데이터는 건들지 않음)
    await DB.execute("DELETE FROM device_secrets WHERE device_id = ?", data.device_id)
    await DB.execute("DELETE FROM device_phones WHERE device_id = ?", data.device_id)
    await DB.execute("UPDATE devices SET family_id = NULL WHERE id = ?", data.device_id)
    await DB.execute("UPDATE join_requests SET status = 'cancelled' WHERE device_id = ? AND status = 'pending'", data.device_id)
    return {"status": "ok"}


# ── 채팅 ──────────────────────────────────────────────────────────────────────

class _ChatConnectionManager:
    """가족 ID별 활성 WebSocket 연결을 관리하고 메시지를 브로드캐스트한다."""

    def __init__(self):
        self._family_conns: Dict[str, Set[Tuple[str, WebSocket]]] = {}
        self._lock = asyncio.Lock()

    async def connect(self, family_id: str, device_id: str, ws: WebSocket):
        async with self._lock:
            self._family_conns.setdefault(family_id, set()).add((device_id, ws))

    async def disconnect(self, family_id: str, device_id: str, ws: WebSocket):
        async with self._lock:
            conns = self._family_conns.get(family_id)
            if not conns:
                return
            conns.discard((device_id, ws))
            if not conns:
                self._family_conns.pop(family_id, None)

    async def broadcast(self, family_id: str, payload: dict):
        text = json.dumps(payload)
        async with self._lock:
            conns = list(self._family_conns.get(family_id, set()))
        for _did, ws in conns:
            try:
                await ws.send_text(text)
            except Exception:
                # 죽은 소켓은 disconnect에서 정리됨
                pass


_chat_manager = _ChatConnectionManager()


async def _fetch_recent_messages(family_id: str, limit: int = 50, since: Optional[str] = None) -> List[dict]:
    if since:
        rows = await DB.fetch(
            "SELECT id, device_id, user_name, content, created_at"
            " FROM chat_messages WHERE family_id = ? AND created_at > ?"
            " ORDER BY created_at ASC",
            family_id, since,
        )
    else:
        # 최신 limit개를 가져온 뒤 시간 오름차순으로 정렬
        rows = await DB.fetch(
            "SELECT id, device_id, user_name, content, created_at"
            " FROM chat_messages WHERE family_id = ?"
            " ORDER BY created_at DESC LIMIT ?",
            family_id, limit,
        )
        rows = list(reversed(rows))

    def _ser(r: dict) -> dict:
        return {k: (v.isoformat() if hasattr(v, 'isoformat') else v) for k, v in r.items()}

    return [_ser(r) for r in rows]


async def _persist_message(family_id: str, device_id: str, user_name: str, content: str) -> dict:
    msg = {
        "id": str(uuid.uuid4()),
        "family_id": family_id,
        "device_id": device_id,
        "user_name": user_name,
        "content": content,
        "created_at": _now_utc(),
    }
    await DB.execute(
        "INSERT INTO chat_messages (id, family_id, device_id, user_name, content, created_at)"
        " VALUES (?, ?, ?, ?, ?, ?)",
        msg["id"], msg["family_id"], msg["device_id"], msg["user_name"], msg["content"], msg["created_at"],
    )
    return msg


@app.get("/api/family/messages")
async def get_messages(device_id: str, limit: int = 50, since: Optional[str] = None):
    device = await _get_device(device_id)
    family_id = device.get("family_id")
    if not family_id:
        return {"messages": []}
    limit = max(1, min(limit, 200))
    messages = await _fetch_recent_messages(family_id, limit=limit, since=since)
    return {"messages": messages}


@app.post("/api/family/messages")
async def post_message(data: ChatSendReq):
    content = data.content.strip()
    if not content:
        raise HTTPException(status_code=400, detail="메시지가 비어 있어요")
    if len(content) > 2000:
        raise HTTPException(status_code=400, detail="메시지가 너무 길어요 (최대 2000자)")

    device = await _get_device(data.device_id)
    family_id = await _require_family(device)

    msg = await _persist_message(family_id, data.device_id, device["user_name"], content)
    # WS로 가족 멤버에게 브로드캐스트 (보낸 사람 포함 — 클라이언트가 tmp 메시지를 확정으로 교체)
    await _chat_manager.broadcast(family_id, {
        "type": "message",
        "id": msg["id"], "device_id": msg["device_id"], "user_name": msg["user_name"],
        "content": msg["content"], "created_at": msg["created_at"],
    })
    return {"message": {k: msg[k] for k in ("id", "device_id", "user_name", "content", "created_at")}}


@app.websocket("/ws/chat/{device_id}")
async def chat_ws(websocket: WebSocket, device_id: str, secret: str = Query(default="")):
    """가족 채팅 WebSocket. URL: /ws/chat/{device_id}?secret={device_secret}"""
    # HTTP 미들웨어는 WS를 거치지 않으므로 여기서 직접 인증
    row = await DB.fetchrow("SELECT secret FROM device_secrets WHERE device_id = ?", device_id)
    if not row or row["secret"] != secret:
        await websocket.close(code=4401)  # 4xxx = 애플리케이션 정의 close code
        return

    device = await DB.fetchrow("SELECT family_id, user_name FROM devices WHERE id = ?", device_id)
    if not device or not device.get("family_id"):
        await websocket.close(code=4403)
        return

    family_id = device["family_id"]
    user_name = device["user_name"]

    await websocket.accept()
    await _chat_manager.connect(family_id, device_id, websocket)

    try:
        # 연결 직후 최근 히스토리 전송
        history = await _fetch_recent_messages(family_id, limit=50)
        await websocket.send_text(json.dumps({"type": "history_batch", "messages": history}))

        while True:
            raw = await websocket.receive_text()
            try:
                data = json.loads(raw)
            except Exception:
                continue
            content = (data.get("content") or "").strip()
            if not content or len(content) > 2000:
                continue

            msg = await _persist_message(family_id, device_id, user_name, content)
            await _chat_manager.broadcast(family_id, {
                "type": "message",
                "id": msg["id"], "device_id": msg["device_id"], "user_name": msg["user_name"],
                "content": msg["content"], "created_at": msg["created_at"],
            })
    except WebSocketDisconnect:
        pass
    except Exception:
        pass
    finally:
        await _chat_manager.disconnect(family_id, device_id, websocket)


# ── 미션 & 포상 ────────────────────────────────────────────────────────────────

async def _require_same_family(actor_device_id: str, family_id: str):
    """actor가 해당 family의 멤버인지 검증."""
    device = await _get_device(actor_device_id)
    if device.get("family_id") != family_id:
        raise HTTPException(status_code=403, detail="같은 가족이 아니에요")
    return device


async def _family_role(device_id: str, family_id: str) -> str:
    """가족 내 역할. 'leader' | 'parent' | 'member'.

    · leader — 가족 대표. **항상 한 명**이고 families.created_by가 유일한 근거다.
      여럿을 허용했더니 "리더 인수"가 이전이 아니라 추가가 돼서 둘 다 리더로 보였다.
    · parent — 부모. 미션·보상·승인 등 관리 권한은 리더와 같다. 여러 명 가능.
    · member — 자녀.
    """
    family = await DB.fetchrow("SELECT created_by FROM families WHERE id = ?", family_id)
    if family and family["created_by"] == device_id:
        return "leader"
    row = await DB.fetchrow("SELECT family_role FROM devices WHERE id = ?", device_id)
    raw = row["family_role"] if row else None
    # 예전 스키마에서 'leader'로 저장된 사람은 관리 권한을 갖고 있었으므로
    # 권한을 잃지 않게 parent로 읽는다. 대표 자리는 created_by가 갖는다.
    if raw == "leader":
        return "parent"
    return raw if raw in ("parent", "member") else "member"


async def _is_leader(device_id: str, family_id: str) -> bool:
    """가족 대표인지 — 대표 자리 자체를 다루는 작업에만 쓴다."""
    return await _family_role(device_id, family_id) == "leader"


async def _can_manage(device_id: str, family_id: str) -> bool:
    """미션·보상·승인 같은 관리 권한. 부모라면 리더가 아니어도 할 수 있다."""
    return await _family_role(device_id, family_id) in ("leader", "parent")


async def _require_admin(actor_device_id: str, family_id: str):
    """관리 권한(리더 또는 부모) 검증."""
    if not await _can_manage(actor_device_id, family_id):
        raise HTTPException(status_code=403, detail="부모(관리 권한)만 할 수 있는 작업이에요")


async def _require_owner_or_leader(actor_device_id: str, family_id: str, owner_device_id: str):
    """만든 사람 본인이거나 리더면 통과.

    미션은 부모가 아이에게도, 아이가 부모에게도 줄 수 있다. 그래서 "내가 낸 미션"은
    내가 고치고 승인할 수 있어야 하고, 리더는 가족 전체를 정리할 수 있어야 한다.
    """
    if actor_device_id == owner_device_id:
        return
    if not await _is_leader(actor_device_id, family_id):
        raise HTTPException(
            status_code=403, detail="이 미션을 만든 사람이나 가족 리더만 할 수 있어요")


async def _get_balance(family_id: str, device_id: str) -> int:
    row = await DB.fetchrow(
        "SELECT balance FROM family_points WHERE family_id = ? AND device_id = ?",
        family_id, device_id,
    )
    return int(row["balance"]) if row else 0


async def _current_device_of_user(user_id: str, family_id: str) -> Optional[str]:
    """그 사용자가 이 가족에서 **지금 쓰는** 기기. 기기를 바꿔도 사람은 그대로라서,
    포상 대상은 항상 여기를 거쳐 찾는다. 여러 개면 가장 최근에 인증한 기기."""
    row = await DB.fetchrow(
        "SELECT d.id FROM devices d JOIN device_secrets ds ON ds.device_id = d.id "
        "WHERE ds.user_id = ? AND d.family_id = ? "
        "ORDER BY ds.created_at DESC LIMIT 1",
        user_id, family_id,
    )
    return row["id"] if row else None


async def _resolve_target_device(
    family_id: str, target_user_id: Optional[str], target_device_id: Optional[str]
) -> str:
    """포상 대상을 기기 id로 확정한다. user_id가 있으면 그쪽이 우선."""
    if target_user_id:
        resolved = await _current_device_of_user(target_user_id, family_id)
        if resolved:
            return resolved
        # 사용자는 아는데 이 가족에 쓰는 기기가 없다 — 탈퇴했거나 아직 재로그인 전.
        if not target_device_id:
            raise HTTPException(
                status_code=404,
                detail="그 가족의 기기를 찾을 수 없어요. 상대가 다시 로그인하면 연결돼요",
            )
    if not target_device_id:
        raise HTTPException(
            status_code=400, detail="포상 대상(target_user_id)이 필요해요"
        )
    return target_device_id


async def _adjust_balance(family_id: str, device_id: str, delta: int):
    """포인트 증감 — UPSERT 패턴."""
    if _USE_PG:
        await DB.execute("""
            INSERT INTO family_points (family_id, device_id, balance, updated_at)
            VALUES ($1, $2, $3, NOW())
            ON CONFLICT (family_id, device_id) DO UPDATE
            SET balance = family_points.balance + EXCLUDED.balance,
                updated_at = EXCLUDED.updated_at
        """, family_id, device_id, delta)
    else:
        existing = await DB.fetchrow(
            "SELECT balance FROM family_points WHERE family_id = ? AND device_id = ?",
            family_id, device_id,
        )
        if existing:
            await DB.execute(
                "UPDATE family_points SET balance = balance + ?, updated_at = ? WHERE family_id = ? AND device_id = ?",
                delta, _now(), family_id, device_id,
            )
        else:
            await DB.execute(
                "INSERT INTO family_points (family_id, device_id, balance, updated_at) VALUES (?, ?, ?, ?)",
                family_id, device_id, delta, _now(),
            )


def _serialize(row: dict) -> dict:
    return {k: (v.isoformat() if hasattr(v, 'isoformat') else v) for k, v in row.items()}


# ── Phase 2: 반복 미션 / 통계 헬퍼 ────────────────────────────────────────────────

def _norm_recurrence(r: Optional[str]) -> str:
    return r if r in ("none", "daily", "weekly") else "none"


def _period_start(recurrence: str) -> Optional[datetime]:
    now = datetime.now()
    if recurrence == "daily":
        return now.replace(hour=0, minute=0, second=0, microsecond=0)
    if recurrence == "weekly":
        d = now.date()
        st = d - timedelta(days=d.weekday())
        return datetime(st.year, st.month, st.day)
    return None


def _as_local_dt(v) -> Optional[datetime]:
    """completed_at 를 tz-naive 로컬 datetime 으로. (PG=datetime, SQLite=str 모두 대응)"""
    if v is None:
        return None
    if isinstance(v, datetime):
        dt = v
    else:
        try:
            dt = datetime.fromisoformat(str(v).replace("Z", "+00:00"))
        except Exception:
            return None
    if dt.tzinfo is not None:
        dt = dt.astimezone().replace(tzinfo=None)
    return dt


# ── 스티커판 헬퍼 ──────────────────────────────────────────────────────────────

async def _get_sticker_config(family_id: str) -> dict:
    row = await DB.fetchrow("SELECT * FROM sticker_board_configs WHERE family_id = ?", family_id)
    if row:
        return _serialize(row)
    now = _now()
    await DB.execute(
        "INSERT INTO sticker_board_configs (family_id, goal_count, reward_title, updated_at) "
        "VALUES (?, ?, ?, ?)",
        family_id, 10, "소원권", now,
    )
    return {
        "family_id": family_id, "goal_count": 10, "reward_title": "소원권",
        "reward_description": None, "updated_by": None, "updated_at": now,
    }


DEFAULT_POINT_GOAL = 100


async def _issue_wish_ticket(
    family_id: str, device_id: str, source: str, source_id: str, label: str
) -> Optional[dict]:
    """소원 티켓 발급. 같은 source+source_id로는 한 번만 나간다.

    중복 방지를 유니크 인덱스에 맡기지 않고 먼저 조회하는 이유:
    발급 실패가 예외로 튀어 상위 작업(스티커 지급·미션 승인)을 되돌리면 안 된다.
    """
    dup = await DB.fetchrow(
        "SELECT id FROM wish_tickets WHERE family_id = ? AND device_id = ? "
        "AND source = ? AND source_id = ?",
        family_id, device_id, source, source_id)
    if dup:
        return None
    tid = str(uuid.uuid4())
    try:
        await DB.execute(
            "INSERT INTO wish_tickets (id, family_id, device_id, source, source_id, label, created_at) "
            "VALUES (?, ?, ?, ?, ?, ?, ?)",
            tid, family_id, device_id, source, source_id, label, _now())
    except Exception as e:
        print(f"[ticket] 발급 실패 (무시): {e}")
        return None
    await _chat_manager.broadcast(family_id, {
        "type": "wish_ticket",
        "device_id": device_id, "source": source, "label": label,
    })
    row = await DB.fetchrow("SELECT * FROM wish_tickets WHERE id = ?", tid)
    return _serialize(row) if row else None


async def _point_goal(family_id: str) -> dict:
    row = await DB.fetchrow("SELECT * FROM point_goals WHERE family_id = ?", family_id)
    if row:
        return dict(row)
    return {"family_id": family_id, "goal_points": DEFAULT_POINT_GOAL,
            "reward_title": "소원 하나", "updated_at": None}


async def _maybe_issue_point_ticket(family_id: str, device_id: str) -> Optional[dict]:
    """목표 포인트에 도달했으면 티켓을 주고 그만큼 포인트를 쓴다.

    차감하지 않으면 목표를 한 번 넘긴 뒤로는 계속 자격이 유지돼서
    "모아서 쓴다"는 감각이 사라진다. 스티커판이 알을 소비하는 것과 같은 이유다.
    """
    goal = int((await _point_goal(family_id))["goal_points"] or DEFAULT_POINT_GOAL)
    if goal <= 0:
        return None
    balance = await _get_balance(family_id, device_id)
    if balance < goal:
        return None
    await _adjust_balance(family_id, device_id, -goal)
    return await _issue_wish_ticket(
        family_id, device_id, "points", f"{_now()}", f"포인트 {goal}점 달성")


async def _grant_sticker(family_id: str, device_id: str, user_name: str, source: str, source_id: str) -> Optional[dict]:
    """스티커 1개 지급. 목표 개수에 도달하면 오래된 스티커부터 소비하고 소원권을 자동 발급한다."""
    sid = str(uuid.uuid4())
    await DB.execute(
        "INSERT INTO member_stickers (id, family_id, device_id, source, source_id, consumed, created_at) "
        "VALUES (?, ?, ?, ?, ?, 0, ?)",
        sid, family_id, device_id, source, source_id, _now(),
    )
    config = await _get_sticker_config(family_id)
    goal = int(config["goal_count"])
    unconsumed = await DB.fetch(
        "SELECT id FROM member_stickers WHERE family_id = ? AND device_id = ? AND consumed = 0 "
        "ORDER BY created_at",
        family_id, device_id,
    )
    if len(unconsumed) < goal:
        return None

    for row in unconsumed[:goal]:
        await DB.execute("UPDATE member_stickers SET consumed = 1 WHERE id = ?", row["id"])

    cid = str(uuid.uuid4())
    now = _now()
    await DB.execute(
        "INSERT INTO sticker_board_completions "
        "(id, family_id, device_id, user_name, goal_count, reward_title, reward_description, completed_at) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        cid, family_id, device_id, user_name, goal,
        config["reward_title"], config["reward_description"], now,
    )
    # 판을 다 채웠으니 소원을 고를 수 있다.
    await _issue_wish_ticket(
        family_id, device_id, "sticker_board", cid,
        f"포도송이 {goal}알 완성")
    row = await DB.fetchrow("SELECT * FROM sticker_board_completions WHERE id = ?", cid)
    return _serialize(row)


async def _grant_mission_stickers(
    mission: dict, device_id: str, user_name: str, completion_id: str
) -> int:
    """미션 하나에 걸린 포도알을 지급한다. 반환값은 실제로 준 개수.

    어려운 미션일수록 여러 알을 줄 수 있게 mission.reward_stickers를 따른다.
    source_id에 순번을 붙여야 목표 달성 판정이 알 단위로 정확히 돈다.
    """
    count = int(mission.get("reward_stickers") or 1)
    count = max(1, min(count, 10))  # 실수로 큰 값이 들어가도 판을 터뜨리지 않게
    for i in range(count):
        completion = await _grant_sticker(
            mission["family_id"], device_id, user_name, "mission",
            completion_id if i == 0 else f"{completion_id}#{i}",
        )
        await _broadcast_sticker_update(mission["family_id"], completion)
    return count


DAILY_ALL_DONE_BONUS = 5


async def _maybe_grant_daily_event_bonus(family_id: str, device_id: str, day: str) -> int:
    """그 날의 일정이 하나라도 있고 전부 끝났으면 완주 보너스 **포인트** 지급 (1인 1회/일).

    보상 분리 이후 일정에서는 포도알이 나오지 않는다 — 포도알은 미션 몫이다.
    "오늘 걸 다 해냈다"는 성취는 살려야 해서 포인트로 준다.
    중복 지급 판정은 기존 member_stickers 기록을 그대로 쓴다(0개짜리 표식).
    """
    # 승인 대기·거절된 일정은 아직 가족 일정이 아니라 완주 판정에서 뺀다.
    # 넣어두면 아이가 올려둔 대기 일정 때문에 온 가족이 보너스를 못 받는다.
    rows = await DB.fetch(
        "SELECT is_completed FROM shared_events WHERE family_id = ? "
        "AND substr(start_time, 1, 10) = ? AND COALESCE(status, 'approved') = 'approved'",
        family_id, day,
    )
    if not rows or any(int(r["is_completed"] or 0) == 0 for r in rows):
        return 0
    dup = await DB.fetchrow(
        "SELECT id FROM event_completions WHERE family_id = ? AND user_device_id = ? "
        "AND event_id = ?",
        family_id, device_id, f"daily-bonus:{day}",
    )
    if dup:
        return 0

    device = await _get_device(device_id)
    await DB.execute(
        "INSERT INTO event_completions (id, family_id, event_id, event_title, user_device_id, "
        "user_name, status, points_awarded, requested_points, approved_by, completed_at, approved_at) "
        "VALUES (?, ?, ?, ?, ?, ?, 'approved', ?, ?, ?, ?, ?)",
        str(uuid.uuid4()), family_id, f"daily-bonus:{day}", "오늘 일정 완주",
        device_id, device["user_name"], DAILY_ALL_DONE_BONUS, DAILY_ALL_DONE_BONUS,
        device_id, _now(), _now(),
    )
    await _adjust_balance(family_id, device_id, DAILY_ALL_DONE_BONUS)
    return DAILY_ALL_DONE_BONUS


async def _broadcast_sticker_update(family_id: str, completion: Optional[dict] = None):
    try:
        await _chat_manager.broadcast(family_id, {"type": "sticker_board_update", "completion": completion})
    except Exception:
        pass


async def _broadcast_mission_completed(
    family_id: str, device_id: str, user_name: str, title: str, points: int
):
    """미션 완료(승인 확정)를 가족 전체에 알림 — 본인은 축하 연출, 나머지는 축하 알림."""
    try:
        await _chat_manager.broadcast(family_id, {
            "type": "mission_completed",
            "device_id": device_id,
            "user_name": user_name,
            "title": title,
            "points": points,
        })
    except Exception:
        pass


# ── 미션 ──────────────────────────────────────────────────────────────────────

@app.get("/api/family/missions")
async def list_missions(device_id: str):
    device = await _get_device(device_id)
    family_id = await _require_family(device)
    rows = await DB.fetch(
        "SELECT * FROM missions WHERE family_id = ? AND archived = 0 ORDER BY created_at DESC",
        family_id,
    )
    return {"missions": [_serialize(r) for r in rows]}


@app.post("/api/family/missions")
async def create_mission(data: MissionUpsertReq, request: Request):
    auth = request.headers.get("X-Device-ID", "")
    if auth != data.device_id:
        raise HTTPException(status_code=403, detail="본인 기기로만 생성 가능")
    device = await _get_device(data.device_id)
    family_id = await _require_family(device)
    # 미션은 가족 누구나 서로에게 낼 수 있다 (부모→아이, 아이→부모).
    if not data.title.strip():
        raise HTTPException(status_code=400, detail="제목을 입력해주세요")
    # 보상이 포인트에서 포도알로 바뀌었다. reward_points는 레거시라 0이 정상.
    if int(data.reward_stickers or 0) <= 0:
        raise HTTPException(status_code=400, detail="포도알은 1개 이상이어야 해요")

    # 같은 제목·같은 담당자의 살아있는 미션이 이미 있으면 그걸 돌려준다.
    # 저장이 됐는지 몰라 여러 번 누르면 목록에 똑같은 게 여러 개 쌓인다.
    # 담당자 없음을 빈 문자열로 정규화해 비교한다.
    # Postgres는 "? IS NULL"의 타입을 추론하지 못해 IndeterminateDatatypeError가 난다.
    dup = await DB.fetchrow(
        "SELECT * FROM missions WHERE family_id = ? AND archived = 0 AND title = ? "
        "AND COALESCE(assignee_device_id, '') = ?",
        family_id, data.title.strip(), data.assignee_device_id or "")
    if dup:
        return {"mission": _serialize(dup), "status": "already_exists"}

    mid = str(uuid.uuid4())
    rec = _norm_recurrence(data.recurrence)
    stickers = max(1, min(int(data.reward_stickers or 1), 10))
    if _USE_PG:
        await DB.execute("""
            INSERT INTO missions (id, family_id, created_by, title, description,
                reward_points, reward_stickers, assignee_device_id, icon,
                require_approval, recurrence, grants_wish, archived, created_at)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, 0, NOW())
        """, mid, family_id, data.device_id, data.title.strip(), data.description,
            data.reward_points, stickers, data.assignee_device_id, data.icon,
            1 if data.require_approval else 0, rec, 1 if data.grants_wish else 0)
    else:
        await DB.execute("""
            INSERT INTO missions (id, family_id, created_by, title, description,
                reward_points, reward_stickers, assignee_device_id, icon,
                require_approval, recurrence, grants_wish, archived, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0, ?)
        """, mid, family_id, data.device_id, data.title.strip(), data.description,
            data.reward_points, stickers, data.assignee_device_id, data.icon,
            1 if data.require_approval else 0, rec, 1 if data.grants_wish else 0, _now())

    row = await DB.fetchrow("SELECT * FROM missions WHERE id = ?", mid)
    return {"mission": _serialize(row)}


@app.patch("/api/family/missions/{mission_id}")
async def update_mission(mission_id: str, data: MissionUpsertReq, request: Request):
    auth = request.headers.get("X-Device-ID", "")
    if auth != data.device_id:
        raise HTTPException(status_code=403, detail="본인 기기로만 가능")
    existing = await DB.fetchrow("SELECT * FROM missions WHERE id = ?", mission_id)
    if not existing:
        raise HTTPException(status_code=404, detail="미션을 찾을 수 없어요")
    await _require_owner_or_leader(
        data.device_id, existing["family_id"], existing["created_by"])

    await DB.execute("""
        UPDATE missions SET title = ?, description = ?, reward_points = ?,
            reward_stickers = ?, assignee_device_id = ?, icon = ?,
            require_approval = ?, recurrence = ?, grants_wish = ?
        WHERE id = ?
    """, data.title.strip(), data.description, data.reward_points,
        max(1, min(int(data.reward_stickers or 1), 10)),
        data.assignee_device_id, data.icon,
        1 if data.require_approval else 0, _norm_recurrence(data.recurrence),
        1 if data.grants_wish else 0, mission_id)
    row = await DB.fetchrow("SELECT * FROM missions WHERE id = ?", mission_id)
    return {"mission": _serialize(row)}


@app.delete("/api/family/missions/{mission_id}")
async def delete_mission(mission_id: str, device_id: str):
    existing = await DB.fetchrow("SELECT * FROM missions WHERE id = ?", mission_id)
    if not existing:
        return {"status": "ok"}  # idempotent
    await _require_owner_or_leader(device_id, existing["family_id"], existing["created_by"])
    await DB.execute("UPDATE missions SET archived = 1 WHERE id = ?", mission_id)
    return {"status": "ok"}


@app.post("/api/family/missions/{mission_id}/complete")
async def complete_mission(mission_id: str, data: MissionCompleteReq, request: Request):
    auth = request.headers.get("X-Device-ID", "")
    if auth != data.device_id:
        raise HTTPException(status_code=403, detail="본인 기기로만 가능")
    mission = await DB.fetchrow("SELECT * FROM missions WHERE id = ? AND archived = 0", mission_id)
    if not mission:
        raise HTTPException(status_code=404, detail="미션을 찾을 수 없어요")
    device = await _require_same_family(data.device_id, mission["family_id"])

    # 지정 대상자가 있다면 그 사람만 완료 가능
    assignee = mission.get("assignee_device_id")
    if assignee and assignee != data.device_id:
        raise HTTPException(status_code=403, detail="이 미션은 다른 가족에게 배정됐어요")

    # 반복 미션: 현재 주기 내 중복 완료 방지 (daily=오늘, weekly=이번주)
    rec = _norm_recurrence(mission.get("recurrence"))
    ps = _period_start(rec)
    if ps is not None:
        recent = await DB.fetch(
            "SELECT completed_at FROM mission_completions "
            "WHERE mission_id = ? AND user_device_id = ? AND status != 'rejected'",
            mission_id, data.device_id,
        )
        if any((_as_local_dt(r["completed_at"]) or datetime.min) >= ps for r in recent):
            raise HTTPException(
                status_code=400,
                detail="오늘 이미 완료했어요" if rec == "daily" else "이번 주에 이미 완료했어요",
            )

    cid = str(uuid.uuid4())
    require_approval = int(mission.get("require_approval", 1)) == 1
    status = "pending" if require_approval else "approved"
    # 미션 보상은 포도알이다. 포인트는 일정 담당으로 옮겼다.
    # reward_points는 과거 기록 때문에 남아 있을 뿐 더 이상 지급하지 않는다.
    points = 0
    approved_by = None if require_approval else data.device_id
    approved_at = None if require_approval else _now()

    if _USE_PG:
        await DB.execute("""
            INSERT INTO mission_completions (id, mission_id, family_id, user_device_id, user_name,
                status, points_awarded, approved_by, completed_at, approved_at)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, NOW(), $9)
        """, cid, mission_id, mission["family_id"], data.device_id, device["user_name"],
            status, points, approved_by, approved_at)
    else:
        await DB.execute("""
            INSERT INTO mission_completions (id, mission_id, family_id, user_device_id, user_name,
                status, points_awarded, approved_by, completed_at, approved_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, cid, mission_id, mission["family_id"], data.device_id, device["user_name"],
            status, points, approved_by, _now(), approved_at)

    # 자동 승인 미션이면 즉시 포도알 지급
    if not require_approval:
        n = await _grant_mission_stickers(
            mission, data.device_id, device["user_name"], cid)
        await _broadcast_mission_completed(
            mission["family_id"], data.device_id, device["user_name"], mission["title"], 0)
        await DB.execute(
            "UPDATE mission_completions SET stickers_awarded = ? WHERE id = ?", n, cid)
        if int(mission.get("grants_wish") or 0):
            await _issue_wish_ticket(
                mission["family_id"], data.device_id, "mission", cid,
                f"'{mission['title']}' 달성")

    row = await DB.fetchrow("SELECT * FROM mission_completions WHERE id = ?", cid)
    return {"completion": _serialize(row)}


@app.get("/api/family/missions/completions")
async def list_completions(device_id: str, status: Optional[str] = None):
    device = await _get_device(device_id)
    family_id = await _require_family(device)
    if status:
        rows = await DB.fetch(
            "SELECT * FROM mission_completions WHERE family_id = ? AND status = ? ORDER BY completed_at DESC",
            family_id, status,
        )
    else:
        rows = await DB.fetch(
            "SELECT * FROM mission_completions WHERE family_id = ? ORDER BY completed_at DESC LIMIT 100",
            family_id,
        )
    return {"completions": [_serialize(r) for r in rows]}


@app.post("/api/family/missions/completions/{completion_id}/approve")
async def approve_completion(completion_id: str, data: CompletionApproveReq, request: Request):
    auth = request.headers.get("X-Device-ID", "")
    if auth != data.device_id:
        raise HTTPException(status_code=403, detail="본인 기기로만 가능")
    completion = await DB.fetchrow("SELECT * FROM mission_completions WHERE id = ?", completion_id)
    if not completion:
        raise HTTPException(status_code=404, detail="요청을 찾을 수 없어요")
    if completion["status"] != "pending":
        raise HTTPException(status_code=400, detail="이미 처리된 요청이에요")
    mission = await DB.fetchrow("SELECT * FROM missions WHERE id = ?", completion["mission_id"])
    # 미션을 낸 사람이 승인한다. 아이가 부모에게 낸 미션이면 아이가 승인자다.
    await _require_owner_or_leader(
        data.device_id, completion["family_id"],
        mission["created_by"] if mission else completion["user_device_id"])

    if data.action == "approve":
        # 미션 보상은 포도알. 포인트는 일정 쪽에서만 나온다.
        stickers = await _grant_mission_stickers(
            mission or {"family_id": completion["family_id"], "reward_stickers": 1},
            completion["user_device_id"], completion["user_name"], completion_id,
        )
        await DB.execute("""
            UPDATE mission_completions SET status = 'approved', points_awarded = 0,
                stickers_awarded = ?, approved_by = ?, approved_at = ? WHERE id = ?
        """, stickers, data.device_id, _now(), completion_id)
        # 목표 미션이면 소원 티켓도 함께.
        if mission and int(mission.get("grants_wish") or 0):
            await _issue_wish_ticket(
                completion["family_id"], completion["user_device_id"],
                "mission", completion_id, f"'{mission['title']}' 달성")
        await _broadcast_mission_completed(
            completion["family_id"], completion["user_device_id"], completion["user_name"],
            mission["title"] if mission else "미션", 0)
    else:
        # 거절은 끝이 아니라 "다시 해보자"다. 사유를 남겨 아이가 뭘 고쳐야 할지 알게 한다.
        await DB.execute("""
            UPDATE mission_completions SET status = 'rejected',
                approved_by = ?, approved_at = ?, reject_reason = ? WHERE id = ?
        """, data.device_id, _now(), data.reason, completion_id)
        await _chat_manager.broadcast(completion["family_id"], {
            "type": "mission_rejected",
            "target_device_id": completion["user_device_id"],
            "title": mission["title"] if mission else "미션",
            "reason": data.reason,
        })
    return {"status": f"{data.action}d"}


# ── 포상 ──────────────────────────────────────────────────────────────────────

@app.get("/api/family/rewards")
async def list_rewards(device_id: str):
    device = await _get_device(device_id)
    family_id = await _require_family(device)
    rows = await DB.fetch(
        "SELECT * FROM rewards WHERE family_id = ? AND archived = 0 ORDER BY cost_points ASC",
        family_id,
    )
    return {"rewards": [_serialize(r) for r in rows]}


@app.post("/api/family/rewards")
async def create_reward(data: RewardUpsertReq, request: Request):
    auth = request.headers.get("X-Device-ID", "")
    if auth != data.device_id:
        raise HTTPException(status_code=403, detail="본인 기기로만 가능")
    device = await _get_device(data.device_id)
    family_id = await _require_family(device)
    # 보상(가족 공용 상점)은 부모만 만든다. 미션을 모두에게 열 때
    # 이 검사까지 같이 지워져서 자녀도 보상을 만들 수 있었다.
    await _require_admin(data.device_id, family_id)

    if not data.title.strip():
        raise HTTPException(status_code=400, detail="제목을 입력해주세요")
    if data.cost_points <= 0:
        raise HTTPException(status_code=400, detail="필요 포인트는 1 이상이어야 해요")

    rid = str(uuid.uuid4())
    if _USE_PG:
        await DB.execute("""
            INSERT INTO rewards (id, family_id, created_by, title, description,
                cost_points, icon, stock, archived, created_at)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, 0, NOW())
        """, rid, family_id, data.device_id, data.title.strip(), data.description,
            data.cost_points, data.icon, data.stock)
    else:
        await DB.execute("""
            INSERT INTO rewards (id, family_id, created_by, title, description,
                cost_points, icon, stock, archived, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, 0, ?)
        """, rid, family_id, data.device_id, data.title.strip(), data.description,
            data.cost_points, data.icon, data.stock, _now())

    row = await DB.fetchrow("SELECT * FROM rewards WHERE id = ?", rid)
    return {"reward": _serialize(row)}


@app.patch("/api/family/rewards/{reward_id}")
async def update_reward(reward_id: str, data: RewardUpsertReq, request: Request):
    auth = request.headers.get("X-Device-ID", "")
    if auth != data.device_id:
        raise HTTPException(status_code=403, detail="본인 기기로만 가능")
    existing = await DB.fetchrow("SELECT * FROM rewards WHERE id = ?", reward_id)
    if not existing:
        raise HTTPException(status_code=404, detail="포상을 찾을 수 없어요")
    await _require_admin(data.device_id, existing["family_id"])

    await DB.execute("""
        UPDATE rewards SET title = ?, description = ?, cost_points = ?,
            icon = ?, stock = ? WHERE id = ?
    """, data.title.strip(), data.description, data.cost_points,
        data.icon, data.stock, reward_id)
    row = await DB.fetchrow("SELECT * FROM rewards WHERE id = ?", reward_id)
    return {"reward": _serialize(row)}


@app.delete("/api/family/rewards/{reward_id}")
async def delete_reward(reward_id: str, device_id: str):
    existing = await DB.fetchrow("SELECT * FROM rewards WHERE id = ?", reward_id)
    if not existing:
        return {"status": "ok"}
    await _require_admin(device_id, existing["family_id"])
    await DB.execute("UPDATE rewards SET archived = 1 WHERE id = ?", reward_id)
    return {"status": "ok"}


@app.post("/api/family/rewards/{reward_id}/redeem")
async def redeem_reward(reward_id: str, data: RewardRedeemReq, request: Request):
    auth = request.headers.get("X-Device-ID", "")
    if auth != data.device_id:
        raise HTTPException(status_code=403, detail="본인 기기로만 가능")
    reward = await DB.fetchrow("SELECT * FROM rewards WHERE id = ? AND archived = 0", reward_id)
    if not reward:
        raise HTTPException(status_code=404, detail="포상을 찾을 수 없어요")
    device = await _require_same_family(data.device_id, reward["family_id"])

    cost = int(reward["cost_points"])
    balance = await _get_balance(reward["family_id"], data.device_id)
    if balance < cost:
        raise HTTPException(status_code=400, detail=f"포인트가 부족해요 (현재 {balance}p, 필요 {cost}p)")

    stock = reward.get("stock")
    if stock is not None and int(stock) <= 0:
        raise HTTPException(status_code=400, detail="이 포상은 모두 사용됐어요")

    rid = str(uuid.uuid4())
    if _USE_PG:
        await DB.execute("""
            INSERT INTO reward_redemptions (id, reward_id, family_id, user_device_id, user_name,
                status, points_cost, requested_at)
            VALUES ($1, $2, $3, $4, $5, 'pending', $6, NOW())
        """, rid, reward_id, reward["family_id"], data.device_id, device["user_name"], cost)
    else:
        await DB.execute("""
            INSERT INTO reward_redemptions (id, reward_id, family_id, user_device_id, user_name,
                status, points_cost, requested_at)
            VALUES (?, ?, ?, ?, ?, 'pending', ?, ?)
        """, rid, reward_id, reward["family_id"], data.device_id, device["user_name"], cost, _now())

    row = await DB.fetchrow("SELECT * FROM reward_redemptions WHERE id = ?", rid)
    return {"redemption": _serialize(row)}


@app.get("/api/family/rewards/redemptions")
async def list_redemptions(device_id: str, status: Optional[str] = None):
    device = await _get_device(device_id)
    family_id = await _require_family(device)
    if status:
        rows = await DB.fetch(
            "SELECT * FROM reward_redemptions WHERE family_id = ? AND status = ? ORDER BY requested_at DESC",
            family_id, status,
        )
    else:
        rows = await DB.fetch(
            "SELECT * FROM reward_redemptions WHERE family_id = ? ORDER BY requested_at DESC LIMIT 100",
            family_id,
        )
    return {"redemptions": [_serialize(r) for r in rows]}


@app.post("/api/family/rewards/redemptions/{redemption_id}/approve")
async def approve_redemption(redemption_id: str, data: RedemptionApproveReq, request: Request):
    auth = request.headers.get("X-Device-ID", "")
    if auth != data.device_id:
        raise HTTPException(status_code=403, detail="본인 기기로만 가능")
    redemption = await DB.fetchrow("SELECT * FROM reward_redemptions WHERE id = ?", redemption_id)
    if not redemption:
        raise HTTPException(status_code=404, detail="요청을 찾을 수 없어요")
    if redemption["status"] != "pending":
        raise HTTPException(status_code=400, detail="이미 처리된 요청이에요")
    await _require_admin(data.device_id, redemption["family_id"])

    if data.action == "approve":
        # 포인트 잔액 재확인 후 차감 (race condition 방어)
        cost = int(redemption["points_cost"])
        balance = await _get_balance(redemption["family_id"], redemption["user_device_id"])
        if balance < cost:
            raise HTTPException(status_code=400, detail=f"신청자 포인트가 부족해요 (현재 {balance}p)")

        await DB.execute("""
            UPDATE reward_redemptions SET status = 'approved',
                approved_by = ?, approved_at = ? WHERE id = ?
        """, data.device_id, _now(), redemption_id)
        await _adjust_balance(redemption["family_id"], redemption["user_device_id"], -cost)

        # 재고 차감 (있을 때만)
        reward = await DB.fetchrow("SELECT stock FROM rewards WHERE id = ?", redemption["reward_id"])
        if reward and reward.get("stock") is not None:
            await DB.execute(
                "UPDATE rewards SET stock = stock - 1 WHERE id = ?", redemption["reward_id"],
            )
    else:
        await DB.execute("""
            UPDATE reward_redemptions SET status = 'rejected',
                approved_by = ?, approved_at = ? WHERE id = ?
        """, data.device_id, _now(), redemption_id)
    return {"status": f"{data.action}d"}


# ── 포인트 잔액 ────────────────────────────────────────────────────────────────

@app.get("/api/family/points")
async def get_family_points(device_id: str):
    """가족 전체 멤버의 포인트 잔액 목록."""
    device = await _get_device(device_id)
    family_id = await _require_family(device)
    rows = await DB.fetch(
        "SELECT device_id, balance FROM family_points WHERE family_id = ?",
        family_id,
    )
    return {"balances": [{"device_id": r["device_id"], "balance": int(r["balance"])} for r in rows]}


# ── 역할·권한 ─────────────────────────────────────────────────────────────────


class RoleRequestReq(BaseModel):
    device_id: str            # 요청자(현재 리더)
    target_device_id: str
    to_role: str              # 'leader' | 'member'


class RoleAnswerReq(BaseModel):
    device_id: str            # 요청받은 본인
    accept: bool


class DeviceOnlyReq(BaseModel):
    device_id: str


@app.post("/api/family/role/request")
async def request_role_change(data: RoleRequestReq, request: Request):
    """리더가 다른 구성원에게 역할 변경을 제안한다.

    통보가 아니라 제안인 이유: 리더가 되면 승인·관리 책임이 따라오는데,
    본인 모르게 떠안기면 요청이 방치되고 아이는 계속 기다리게 된다.
    """
    auth = request.headers.get("X-Device-ID", "")
    if auth != data.device_id:
        raise HTTPException(status_code=403, detail="본인 기기로만 가능")
    if data.to_role not in {"leader", "parent"}:
        raise HTTPException(
            status_code=400, detail="역할은 leader(대표 양도) 또는 parent(부모 권한)예요")

    actor = await _get_device(data.device_id)
    family_id = await _require_family(actor)
    await _require_admin(data.device_id, family_id)

    target = await _get_device(data.target_device_id)
    if target["family_id"] != family_id:
        raise HTTPException(status_code=403, detail="같은 가족 구성원만 가능해요")
    if data.target_device_id == data.device_id:
        raise HTTPException(status_code=400, detail="자기 역할은 직접 바꿀 수 없어요")

    # 같은 대상에게 이미 대기 중인 요청이 있으면 그것을 돌려준다.
    dup = await DB.fetchrow(
        "SELECT * FROM role_change_requests WHERE family_id = ? AND target_device_id = ? "
        "AND status = 'pending'",
        family_id, data.target_device_id)
    if dup:
        return {"request": _serialize(dup), "status": "already_pending"}

    rid = str(uuid.uuid4())
    await DB.execute(
        "INSERT INTO role_change_requests (id, family_id, target_device_id, requested_by, "
        "requester_name, to_role, status, created_at) VALUES (?, ?, ?, ?, ?, ?, 'pending', ?)",
        rid, family_id, data.target_device_id, data.device_id,
        actor["user_name"], data.to_role, _now())

    await _chat_manager.broadcast(family_id, {
        "type": "role_request",
        "request_id": rid,
        "target_device_id": data.target_device_id,
        "requester_name": actor["user_name"],
        "to_role": data.to_role,
    })
    row = await DB.fetchrow("SELECT * FROM role_change_requests WHERE id = ?", rid)
    return {"request": _serialize(row), "status": "pending"}


@app.get("/api/family/role/requests")
async def list_role_requests(device_id: str):
    """나에게 온 대기 중인 역할 제안."""
    device = await _get_device(device_id)
    family_id = await _require_family(device)
    rows = await DB.fetch(
        "SELECT * FROM role_change_requests WHERE family_id = ? AND target_device_id = ? "
        "AND status = 'pending' ORDER BY created_at DESC",
        family_id, device_id)
    return {"requests": [_serialize(r) for r in rows],
            "my_role": await _family_role(device_id, family_id)}


@app.post("/api/family/role/requests/{request_id}/answer")
async def answer_role_request(request_id: str, data: RoleAnswerReq, request: Request):
    auth = request.headers.get("X-Device-ID", "")
    if auth != data.device_id:
        raise HTTPException(status_code=403, detail="본인 기기로만 가능")
    req = await DB.fetchrow("SELECT * FROM role_change_requests WHERE id = ?", request_id)
    if not req:
        raise HTTPException(status_code=404, detail="요청을 찾을 수 없어요")
    if req["target_device_id"] != data.device_id:
        raise HTTPException(status_code=403, detail="본인에게 온 요청만 답할 수 있어요")
    if req["status"] != "pending":
        raise HTTPException(status_code=400, detail="이미 처리된 요청이에요")

    status = "accepted" if data.accept else "rejected"
    await DB.execute(
        "UPDATE role_change_requests SET status = ?, resolved_at = ? WHERE id = ?",
        status, _now(), request_id)

    if data.accept:
        if req["to_role"] == "leader":
            # 대표 "양도" — 넘기는 사람은 부모로 남아 관리 권한을 유지한다.
            # 예전엔 둘 다 리더가 돼서 인수가 아니라 추가처럼 보였다.
            await DB.execute("UPDATE families SET created_by = ? WHERE id = ?",
                             data.device_id, req["family_id"])
            await DB.execute("UPDATE devices SET family_role = 'parent' WHERE id = ?",
                             req["requested_by"])
            await DB.execute("UPDATE devices SET family_role = 'parent' WHERE id = ?",
                             data.device_id)
        else:
            await DB.execute("UPDATE devices SET family_role = ? WHERE id = ?",
                             req["to_role"], data.device_id)

    device = await _get_device(data.device_id)
    await _chat_manager.broadcast(req["family_id"], {
        "type": "role_changed",
        "device_id": data.device_id,
        "user_name": device["user_name"],
        "accepted": data.accept,
        "role": req["to_role"] if data.accept else None,
    })
    return {"status": status,
            "my_role": await _family_role(data.device_id, req["family_id"])}


class RoleRevokeReq(BaseModel):
    device_id: str            # 대표
    target_device_id: str


@app.post("/api/family/role/revoke-parent")
async def revoke_parent(data: RoleRevokeReq, request: Request):
    """대표가 부모 권한을 거둔다.

    권한을 **주는** 건 수락이 필요하지만 **거두는** 건 즉시다.
    책임을 떠안기는 것과 덜어내는 것은 무게가 다르다.
    """
    auth = request.headers.get("X-Device-ID", "")
    if auth != data.device_id:
        raise HTTPException(status_code=403, detail="본인 기기로만 가능")
    device = await _get_device(data.device_id)
    family_id = await _require_family(device)
    if not await _is_leader(data.device_id, family_id):
        raise HTTPException(status_code=403, detail="가족 대표만 할 수 있어요")
    if data.target_device_id == data.device_id:
        raise HTTPException(status_code=400, detail="자기 권한은 여기서 못 바꿔요")

    target = await _get_device(data.target_device_id)
    if target["family_id"] != family_id:
        raise HTTPException(status_code=403, detail="같은 가족 구성원만 가능해요")

    await DB.execute("UPDATE devices SET family_role = 'member' WHERE id = ?",
                     data.target_device_id)
    # 대기 중인 제안이 있으면 함께 정리한다.
    await DB.execute(
        "UPDATE role_change_requests SET status = 'cancelled', resolved_at = ? "
        "WHERE family_id = ? AND target_device_id = ? AND status = 'pending'",
        _now(), family_id, data.target_device_id)
    await _chat_manager.broadcast(family_id, {
        "type": "role_changed", "device_id": data.target_device_id,
        "user_name": target["user_name"], "accepted": True, "role": "member",
    })
    return {"role": "member"}


@app.post("/api/family/role/resign")
async def resign_parent(data: DeviceOnlyReq, request: Request):
    """부모(관리) 권한을 스스로 내려놓는다.

    대표(leader) 자리는 이 방법으로 비울 수 없다 — 대표가 없는 가족이 생기면
    관리가 잠긴다. 대표는 다른 사람에게 **양도**해야 한다.
    """
    auth = request.headers.get("X-Device-ID", "")
    if auth != data.device_id:
        raise HTTPException(status_code=403, detail="본인 기기로만 가능")
    device = await _get_device(data.device_id)
    family_id = await _require_family(device)
    role = await _family_role(data.device_id, family_id)
    if role == "leader":
        raise HTTPException(
            status_code=400,
            detail="가족 대표는 먼저 다른 가족에게 대표를 넘겨주세요")
    if role != "parent":
        raise HTTPException(status_code=400, detail="이미 부모 권한이 없어요")

    await DB.execute("UPDATE devices SET family_role = 'member' WHERE id = ?", data.device_id)
    await _chat_manager.broadcast(family_id, {
        "type": "role_changed", "device_id": data.device_id,
        "user_name": device["user_name"], "accepted": True, "role": "member",
    })
    return {"my_role": "member"}


# ── 소원 리스트 ───────────────────────────────────────────────────────────────


class PointGoalReq(BaseModel):
    device_id: str
    goal_points: int
    reward_title: Optional[str] = None


@app.get("/api/family/point-goal")
async def get_point_goal(device_id: str):
    device = await _get_device(device_id)
    family_id = await _require_family(device)
    goal = await _point_goal(family_id)
    return {"goal": _serialize(goal), "my_balance": await _get_balance(family_id, device_id)}


@app.patch("/api/family/point-goal")
async def set_point_goal(data: PointGoalReq, request: Request):
    """목표 포인트는 리더가 정한다 — 본인이 정하면 목표가 되지 않는다."""
    auth = request.headers.get("X-Device-ID", "")
    if auth != data.device_id:
        raise HTTPException(status_code=403, detail="본인 기기로만 가능")
    device = await _get_device(data.device_id)
    family_id = await _require_family(device)
    await _require_admin(data.device_id, family_id)
    goal = max(10, min(int(data.goal_points), 100000))

    existing = await DB.fetchrow("SELECT family_id FROM point_goals WHERE family_id = ?", family_id)
    if existing:
        await DB.execute(
            "UPDATE point_goals SET goal_points = ?, reward_title = ?, updated_at = ? "
            "WHERE family_id = ?",
            goal, data.reward_title, _now(), family_id)
    else:
        await DB.execute(
            "INSERT INTO point_goals (family_id, goal_points, reward_title, updated_at) "
            "VALUES (?, ?, ?, ?)",
            family_id, goal, data.reward_title, _now())
    await _chat_manager.broadcast(family_id, {"type": "point_goal_update"})
    return {"goal": _serialize(await _point_goal(family_id))}


class WishUpsertReq(BaseModel):
    device_id: str
    title: str
    emoji: Optional[str] = None
    note: Optional[str] = None


class WishGrantReq(BaseModel):
    device_id: str            # 요청/약속하는 사람
    wish_id: str
    source: Optional[str] = None   # 'sticker_board' | 'points' | 'mission'
    note: Optional[str] = None


class WishGrantDecideReq(BaseModel):
    device_id: str
    action: str               # 'promise' | 'fulfill' | 'reject'
    note: Optional[str] = None


@app.get("/api/family/wishes")
async def list_wishes(device_id: str):
    """가족 전체의 소원 목록. 부모가 아이 리스트에서 고를 수 있어야 해서 전부 준다."""
    device = await _get_device(device_id)
    family_id = await _require_family(device)
    rows = await DB.fetch(
        "SELECT * FROM member_wishes WHERE family_id = ? AND archived = 0 ORDER BY created_at",
        family_id)
    # ticket_source: 이 약속에 쓰인 소원권의 출처('sticker_board' 등). 없으면 아직
    # 목표를 채우기 전에 미리 한 약속이다 — 포도송이 화면이 "다 채우면 받을 소원"과
    # "채워서 받을 차례인 소원"을 구분하는 데 쓴다.
    grants = await DB.fetch(
        "SELECT g.*, (SELECT t.source FROM wish_tickets t WHERE t.used_grant_id = g.id"
        " LIMIT 1) AS ticket_source"
        " FROM wish_grants g WHERE g.family_id = ? ORDER BY g.created_at DESC LIMIT 100",
        family_id)
    tickets = await DB.fetch(
        "SELECT * FROM wish_tickets WHERE family_id = ? AND used_at IS NULL "
        "ORDER BY created_at DESC",
        family_id)
    return {"wishes": [_serialize(r) for r in rows],
            "grants": [_serialize(g) for g in grants],
            # 가족 전체 티켓 — 부모가 "아이가 소원 고를 수 있는 상태"를 알아야 한다.
            "tickets": [_serialize(t) for t in tickets],
            "point_goal": _serialize(await _point_goal(family_id))}


@app.post("/api/family/wishes")
async def create_wish(data: WishUpsertReq, request: Request):
    """소원은 **본인만** 자기 리스트에 추가한다 — 받고 싶은 걸 정하는 건 본인이다."""
    auth = request.headers.get("X-Device-ID", "")
    if auth != data.device_id:
        raise HTTPException(status_code=403, detail="본인 기기로만 가능")
    if not data.title.strip():
        raise HTTPException(status_code=400, detail="소원 내용을 적어주세요")
    device = await _get_device(data.device_id)
    family_id = await _require_family(device)

    wid = str(uuid.uuid4())
    await DB.execute(
        "INSERT INTO member_wishes (id, family_id, device_id, title, emoji, note, archived, created_at) "
        "VALUES (?, ?, ?, ?, ?, ?, 0, ?)",
        wid, family_id, data.device_id, data.title.strip(), data.emoji, data.note, _now())
    row = await DB.fetchrow("SELECT * FROM member_wishes WHERE id = ?", wid)
    await _chat_manager.broadcast(family_id, {"type": "wish_update"})
    return {"wish": _serialize(row)}


@app.delete("/api/family/wishes/{wish_id}")
async def delete_wish(wish_id: str, device_id: str):
    device = await _get_device(device_id)
    family_id = await _require_family(device)
    wish = await DB.fetchrow("SELECT * FROM member_wishes WHERE id = ?", wish_id)
    if not wish or wish["family_id"] != family_id:
        raise HTTPException(status_code=404, detail="소원을 찾을 수 없어요")
    # 남의 소원을 지울 수 있으면 리스트의 의미가 없다. 리더도 예외가 아니다.
    if wish["device_id"] != device_id:
        raise HTTPException(status_code=403, detail="자기 소원만 지울 수 있어요")
    await DB.execute("UPDATE member_wishes SET archived = 1 WHERE id = ?", wish_id)
    await _chat_manager.broadcast(family_id, {"type": "wish_update"})
    return {"status": "ok"}


@app.post("/api/family/wishes/grant")
async def grant_wish(data: WishGrantReq, request: Request):
    """소원 지급 흐름의 시작점.

    · 본인이 자기 소원을 고르면 → 'requested' (부모 확인 대기)
    · 리더가 남의 소원을 고르면 → 'promised' (아이가 아직 안 골랐을 때 먼저 약속)
    """
    auth = request.headers.get("X-Device-ID", "")
    if auth != data.device_id:
        raise HTTPException(status_code=403, detail="본인 기기로만 가능")
    actor = await _get_device(data.device_id)
    family_id = await _require_family(actor)

    wish = await DB.fetchrow("SELECT * FROM member_wishes WHERE id = ?", data.wish_id)
    if not wish or wish["family_id"] != family_id or int(wish["archived"] or 0):
        raise HTTPException(status_code=404, detail="소원을 찾을 수 없어요")

    owner_id = wish["device_id"]
    is_own = owner_id == data.device_id
    # 대표뿐 아니라 **부모**도 아이 소원을 골라줄 수 있다.
    # 앱은 isAdmin(대표+부모)에게 "약속하기"를 보여주는데 여기서 대표만 통과시켜서,
    # 부모가 누르면 403이 나는 상태였다 — 권한 체계(대표/부모/구성원)와 어긋났다.
    if not is_own and not await _can_manage(data.device_id, family_id):
        raise HTTPException(status_code=403, detail="부모(관리 권한)만 다른 사람의 소원을 약속할 수 있어요")

    owner = await _get_device(owner_id)

    # 티켓이 있으면 그걸 쓴다. 목표를 달성해서 얻은 권리를 소비하는 것이라
    # 없으면 그냥 일반 요청으로 둔다(부모가 판단).
    ticket = await DB.fetchrow(
        "SELECT * FROM wish_tickets WHERE family_id = ? AND device_id = ? AND used_at IS NULL "
        "ORDER BY created_at LIMIT 1",
        family_id, owner_id)

    gid = str(uuid.uuid4())
    status = "requested" if is_own else "promised"
    now = _now()
    await DB.execute(
        "INSERT INTO wish_grants (id, family_id, wish_id, wish_title, owner_device_id, "
        "owner_name, status, source, note, requested_by, decided_by, created_at, updated_at) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        gid, family_id, data.wish_id, wish["title"], owner_id, owner["user_name"],
        status, data.source or (ticket["source"] if ticket else None),
        data.note, data.device_id,
        None if is_own else data.device_id, now, now)

    if ticket:
        await DB.execute(
            "UPDATE wish_tickets SET used_at = ?, used_grant_id = ? WHERE id = ?",
            now, gid, ticket["id"])

    await _chat_manager.broadcast(family_id, {
        "type": "wish_grant",
        "grant_id": gid, "status": status,
        "owner_device_id": owner_id, "owner_name": owner["user_name"],
        "wish_title": wish["title"], "actor_name": actor["user_name"],
    })
    row = await DB.fetchrow("SELECT * FROM wish_grants WHERE id = ?", gid)
    return {"grant": _serialize(row)}


@app.post("/api/family/wishes/grants/{grant_id}/decide")
async def decide_wish_grant(grant_id: str, data: WishGrantDecideReq, request: Request):
    """리더가 소원 요청을 약속(promise)하거나 들어줬다고 표시(fulfill)하거나 거절한다."""
    auth = request.headers.get("X-Device-ID", "")
    if auth != data.device_id:
        raise HTTPException(status_code=403, detail="본인 기기로만 가능")
    if data.action not in {"promise", "fulfill", "reject"}:
        raise HTTPException(status_code=400, detail="action은 promise/fulfill/reject")

    g = await DB.fetchrow("SELECT * FROM wish_grants WHERE id = ?", grant_id)
    if not g:
        raise HTTPException(status_code=404, detail="요청을 찾을 수 없어요")
    await _require_admin(data.device_id, g["family_id"])
    if g["status"] in {"fulfilled", "rejected"}:
        raise HTTPException(status_code=400, detail="이미 마무리된 요청이에요")

    status = {"promise": "promised", "fulfill": "fulfilled", "reject": "rejected"}[data.action]
    await DB.execute(
        "UPDATE wish_grants SET status = ?, decided_by = ?, note = COALESCE(?, note), "
        "updated_at = ? WHERE id = ?",
        status, data.device_id, data.note, _now(), grant_id)

    await _chat_manager.broadcast(g["family_id"], {
        "type": "wish_grant",
        "grant_id": grant_id, "status": status,
        "owner_device_id": g["owner_device_id"], "owner_name": g["owner_name"],
        "wish_title": g["wish_title"],
    })
    return {"status": status}


# ── 위치 공유 ─────────────────────────────────────────────────────────────────

LOCATION_LOG_RETENTION_DAYS = 30


class LocationConsentReq(BaseModel):
    device_id: str
    agreed: bool
    paused: bool = False


class PlaceUpsertReq(BaseModel):
    device_id: str
    name: str
    emoji: Optional[str] = None
    lat: float
    lng: float
    radius_m: int = 150


class LocationEventReq(BaseModel):
    device_id: str
    kind: str                     # 'arrive' | 'leave'
    place_id: Optional[str] = None
    place_name: Optional[str] = None
    lat: Optional[float] = None
    lng: Optional[float] = None
    occurred_at: Optional[str] = None


class LocationPingReq(BaseModel):
    device_id: str                # 요청자
    target_device_id: str


class LocationPingAnswerReq(BaseModel):
    device_id: str                # 응답자(본인)
    lat: float
    lng: float
    accuracy_m: Optional[float] = None


async def _location_consent(family_id: str, device_id: str) -> dict:
    row = await DB.fetchrow(
        "SELECT * FROM location_consents WHERE family_id = ? AND device_id = ?",
        family_id, device_id,
    )
    return dict(row) if row else {"agreed": 0, "paused": 0}


async def _require_location_consent(family_id: str, device_id: str):
    """동의하지 않았거나 일시정지 중이면 위치를 받지도, 주지도 않는다."""
    c = await _location_consent(family_id, device_id)
    if not int(c.get("agreed") or 0):
        raise HTTPException(status_code=403, detail="위치 공유에 동의하지 않은 사용자예요")
    if int(c.get("paused") or 0):
        raise HTTPException(status_code=409, detail="지금은 위치 공유를 잠시 멈춰뒀어요")


@app.get("/api/family/location/state")
async def location_state(device_id: str):
    """내 동의 상태 + 나를 볼 수 있는 사람 + 최근 나를 조회한 기록.

    "누가 나를 보는지"를 언제나 확인할 수 있어야 몰래 보는 앱이 되지 않는다.
    """
    device = await _get_device(device_id)
    family_id = await _require_family(device)
    mine = await _location_consent(family_id, device_id)
    _, members = await _get_family_members(family_id)
    rows = await DB.fetch(
        "SELECT * FROM location_consents WHERE family_id = ?", family_id)
    consent_by_device = {r["device_id"]: r for r in rows}
    recent = await DB.fetch(
        "SELECT * FROM location_pings WHERE family_id = ? AND target_device_id = ? "
        "ORDER BY requested_at DESC LIMIT 20",
        family_id, device_id,
    )
    return {
        "agreed": bool(int(mine.get("agreed") or 0)),
        "paused": bool(int(mine.get("paused") or 0)),
        # 나를 볼 수 있는 사람 = 같은 가족이면서 나도 동의한 상태일 때의 구성원들
        "visible_to": [
            {"device_id": m["device_id"], "user_name": m["user_name"]}
            for m in members
            if m["device_id"] != device_id and int(mine.get("agreed") or 0)
        ],
        "members": [
            {
                "device_id": m["device_id"],
                "user_name": m["user_name"],
                "sharing": bool(int((consent_by_device.get(m["device_id"]) or {}).get("agreed") or 0))
                and not bool(int((consent_by_device.get(m["device_id"]) or {}).get("paused") or 0)),
            }
            for m in members
        ],
        "recent_lookups": [_serialize(r) for r in recent],
    }


@app.post("/api/family/location/consent")
async def set_location_consent(data: LocationConsentReq, request: Request):
    auth = request.headers.get("X-Device-ID", "")
    if auth != data.device_id:
        raise HTTPException(status_code=403, detail="본인 기기로만 가능")
    device = await _get_device(data.device_id)
    family_id = await _require_family(device)
    now = _now()

    existing = await DB.fetchrow(
        "SELECT device_id FROM location_consents WHERE family_id = ? AND device_id = ?",
        family_id, data.device_id,
    )
    if existing:
        await DB.execute(
            "UPDATE location_consents SET agreed = ?, paused = ?, updated_at = ? "
            "WHERE family_id = ? AND device_id = ?",
            1 if data.agreed else 0, 1 if data.paused else 0, now, family_id, data.device_id,
        )
    else:
        await DB.execute(
            "INSERT INTO location_consents (family_id, device_id, agreed, paused, agreed_at, updated_at) "
            "VALUES (?, ?, ?, ?, ?, ?)",
            family_id, data.device_id, 1 if data.agreed else 0,
            1 if data.paused else 0, now if data.agreed else None, now,
        )

    # 동의를 끄거나 멈추면 이미 쌓인 기록도 지운다 — 끈 뒤에도 남아 있으면 신뢰가 깨진다.
    if not data.agreed:
        await DB.execute(
            "DELETE FROM location_events WHERE family_id = ? AND device_id = ?",
            family_id, data.device_id)
        await DB.execute(
            "DELETE FROM location_pings WHERE family_id = ? AND target_device_id = ?",
            family_id, data.device_id)

    # 공유를 멈춘 사실은 가족에게 알린다. 조용히 사라지면 오해가 생긴다.
    await _chat_manager.broadcast(family_id, {
        "type": "location_sharing_changed",
        "device_id": data.device_id,
        "user_name": device["user_name"],
        "agreed": data.agreed,
        "paused": data.paused,
    })
    return {"agreed": data.agreed, "paused": data.paused}


@app.get("/api/family/places")
async def list_places(device_id: str):
    device = await _get_device(device_id)
    family_id = await _require_family(device)
    rows = await DB.fetch(
        "SELECT * FROM family_places WHERE family_id = ? ORDER BY created_at", family_id)
    return {"places": [_serialize(r) for r in rows]}


@app.post("/api/family/places")
async def create_place(data: PlaceUpsertReq, request: Request):
    auth = request.headers.get("X-Device-ID", "")
    if auth != data.device_id:
        raise HTTPException(status_code=403, detail="본인 기기로만 가능")
    device = await _get_device(data.device_id)
    family_id = await _require_family(device)
    pid = str(uuid.uuid4())
    await DB.execute(
        "INSERT INTO family_places (id, family_id, name, emoji, lat, lng, radius_m, created_by, created_at) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
        pid, family_id, data.name.strip(), data.emoji, data.lat, data.lng,
        max(50, min(data.radius_m, 2000)), data.device_id, _now(),
    )
    row = await DB.fetchrow("SELECT * FROM family_places WHERE id = ?", pid)
    return {"place": _serialize(row)}


@app.delete("/api/family/places/{place_id}")
async def delete_place(place_id: str, device_id: str):
    device = await _get_device(device_id)
    family_id = await _require_family(device)
    await DB.execute(
        "DELETE FROM family_places WHERE id = ? AND family_id = ?", place_id, family_id)
    return {"status": "ok"}


@app.post("/api/family/location/events")
async def record_location_event(data: LocationEventReq, request: Request):
    """도착/출발 기록. 좌표가 아니라 '어느 장소에' 를 남기는 게 기본이다."""
    auth = request.headers.get("X-Device-ID", "")
    if auth != data.device_id:
        raise HTTPException(status_code=403, detail="본인 기기로만 가능")
    if data.kind not in {"arrive", "leave"}:
        raise HTTPException(status_code=400, detail="kind는 arrive 또는 leave")

    device = await _get_device(data.device_id)
    family_id = await _require_family(device)
    await _require_location_consent(family_id, data.device_id)

    await DB.execute(
        "INSERT INTO location_events (id, family_id, device_id, user_name, place_id, "
        "place_name, kind, lat, lng, occurred_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        str(uuid.uuid4()), family_id, data.device_id, device["user_name"],
        data.place_id, data.place_name, data.kind, data.lat, data.lng,
        data.occurred_at or _now(),
    )
    # 보관 기간을 넘긴 기록은 그때그때 지운다 — 오래된 동선이 남아 있을 이유가 없다.
    cutoff = (datetime.now() - timedelta(days=LOCATION_LOG_RETENTION_DAYS)).isoformat()
    await DB.execute(
        "DELETE FROM location_events WHERE family_id = ? AND occurred_at < ?", family_id, cutoff)

    await _chat_manager.broadcast(family_id, {
        "type": "location_event",
        "device_id": data.device_id,
        "user_name": device["user_name"],
        "place_name": data.place_name,
        "kind": data.kind,
    })
    return {"status": "ok"}


@app.get("/api/family/location/events")
async def list_location_events(device_id: str, day: Optional[str] = None):
    device = await _get_device(device_id)
    family_id = await _require_family(device)
    if day:
        rows = await DB.fetch(
            "SELECT * FROM location_events WHERE family_id = ? AND substr(occurred_at,1,10) = ? "
            "ORDER BY occurred_at DESC", family_id, day)
    else:
        rows = await DB.fetch(
            "SELECT * FROM location_events WHERE family_id = ? ORDER BY occurred_at DESC LIMIT 200",
            family_id)
    return {"events": [_serialize(r) for r in rows],
            "retention_days": LOCATION_LOG_RETENTION_DAYS}


@app.post("/api/family/location/ping")
async def request_location_ping(data: LocationPingReq, request: Request):
    """"지금 어디야?" — 상대 기기가 응답해야만 좌표가 생긴다."""
    auth = request.headers.get("X-Device-ID", "")
    if auth != data.device_id:
        raise HTTPException(status_code=403, detail="본인 기기로만 가능")
    requester = await _get_device(data.device_id)
    family_id = await _require_family(requester)
    target = await _get_device(data.target_device_id)
    if target["family_id"] != family_id:
        raise HTTPException(status_code=403, detail="같은 가족에게만 요청할 수 있어요")
    await _require_location_consent(family_id, data.target_device_id)

    pid = str(uuid.uuid4())
    await DB.execute(
        "INSERT INTO location_pings (id, family_id, requested_by, requester_name, "
        "target_device_id, status, requested_at) VALUES (?, ?, ?, ?, ?, 'pending', ?)",
        pid, family_id, data.device_id, requester["user_name"],
        data.target_device_id, _now(),
    )
    # 조회 사실을 당사자에게 반드시 알린다 — 몰래 보는 기능이 아니다.
    await _chat_manager.broadcast(family_id, {
        "type": "location_ping_requested",
        "ping_id": pid,
        "target_device_id": data.target_device_id,
        "requester_name": requester["user_name"],
    })
    return {"ping_id": pid, "status": "pending"}


@app.post("/api/family/location/ping/{ping_id}/answer")
async def answer_location_ping(ping_id: str, data: LocationPingAnswerReq, request: Request):
    auth = request.headers.get("X-Device-ID", "")
    if auth != data.device_id:
        raise HTTPException(status_code=403, detail="본인 기기로만 가능")
    ping = await DB.fetchrow("SELECT * FROM location_pings WHERE id = ?", ping_id)
    if not ping:
        raise HTTPException(status_code=404, detail="요청을 찾을 수 없어요")
    if ping["target_device_id"] != data.device_id:
        raise HTTPException(status_code=403, detail="본인에게 온 요청만 응답할 수 있어요")
    await _require_location_consent(ping["family_id"], data.device_id)

    await DB.execute(
        "UPDATE location_pings SET status = 'answered', lat = ?, lng = ?, "
        "accuracy_m = ?, answered_at = ? WHERE id = ?",
        data.lat, data.lng, data.accuracy_m, _now(), ping_id,
    )
    await _chat_manager.broadcast(ping["family_id"], {
        "type": "location_ping_answered",
        "ping_id": ping_id,
        "target_device_id": data.device_id,
        "lat": data.lat, "lng": data.lng,
    })
    return {"status": "answered"}


@app.get("/api/family/location/pings")
async def list_location_pings(device_id: str, mine: bool = False):
    """mine=true면 나에게 온 요청(응답해야 할 것), 아니면 내가 보낸 요청."""
    device = await _get_device(device_id)
    family_id = await _require_family(device)
    if mine:
        rows = await DB.fetch(
            "SELECT * FROM location_pings WHERE family_id = ? AND target_device_id = ? "
            "AND status = 'pending' ORDER BY requested_at DESC LIMIT 20",
            family_id, device_id)
    else:
        rows = await DB.fetch(
            "SELECT * FROM location_pings WHERE family_id = ? AND requested_by = ? "
            "ORDER BY requested_at DESC LIMIT 20", family_id, device_id)
    return {"pings": [_serialize(r) for r in rows]}


@app.post("/api/family/events/complete")
async def complete_event(data: EventCompleteReq, request: Request):
    """일정 완료 → 포인트 지급 또는 부모 확인 요청.

    승인을 걸지 말지는 **누가 눌렀는가**로 가른다. 모든 완료에 승인을 요구하면
    부모가 자기 일정을 끝낼 때도 자기 승인을 기다리게 되어 앱이 성가셔진다.
      · 방장(부모)이 눌렀다  → 이미 확인한 셈이므로 즉시 지급
      · 담당자가 아닌 사람이 눌렀다 → 대신 확인해 준 것이므로 즉시 지급
      · 아이가 자기 일정을 눌렀다   → 부모 확인 대기
    """
    auth = request.headers.get("X-Device-ID", "")
    if auth != data.device_id:
        raise HTTPException(status_code=403, detail="본인 기기로만 가능")

    actor = await _get_device(data.device_id)
    family_id = await _require_family(actor)
    points = max(0, int(data.points or 0))

    target_device_id = data.device_id
    if data.target_user_id:
        resolved = await _current_device_of_user(data.target_user_id, family_id)
        if resolved:
            target_device_id = resolved

    family = await DB.fetchrow("SELECT created_by FROM families WHERE id = ?", family_id)
    is_admin = family and family["created_by"] == data.device_id
    completing_own = target_device_id == data.device_id

    # 이미 처리된 일정이면 그대로 돌려준다 (완료/취소를 반복해도 한 번만).
    existing = await DB.fetchrow(
        "SELECT * FROM event_completions WHERE family_id = ? AND event_id = ? AND user_device_id = ?",
        family_id, data.event_id, target_device_id,
    )
    if existing and existing["status"] != "rejected":
        return {"status": "already_" + existing["status"], "completion": _serialize(existing)}

    target = await _get_device(target_device_id)
    needs_approval = completing_own and not is_admin

    cid = existing["id"] if existing else str(uuid.uuid4())
    status = "pending" if needs_approval else "approved"
    awarded = 0 if needs_approval else points

    if existing:
        await DB.execute(
            "UPDATE event_completions SET status = ?, points_awarded = ?, requested_points = ?, "
            "approved_by = ?, reject_reason = NULL, completed_at = ?, approved_at = ? WHERE id = ?",
            status, awarded, points, None if needs_approval else data.device_id,
            _now(), None if needs_approval else _now(), cid,
        )
    else:
        await DB.execute(
            "INSERT INTO event_completions (id, family_id, event_id, event_title, user_device_id, "
            "user_name, status, points_awarded, requested_points, approved_by, completed_at, approved_at) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            cid, family_id, data.event_id, data.event_title, target_device_id,
            target["user_name"], status, awarded, points,
            None if needs_approval else data.device_id, _now(),
            None if needs_approval else _now(),
        )

    if not needs_approval and awarded > 0:
        await _adjust_balance(family_id, target_device_id, awarded)
        await _maybe_issue_point_ticket(family_id, target_device_id)

    await _chat_manager.broadcast(family_id, {
        "type": "event_completion",
        "status": status,
        "target_device_id": target_device_id,
        "target_name": target["user_name"],
        "actor_name": actor["user_name"],
        "event_id": data.event_id,
        "event_title": data.event_title,
        "points": awarded,
    })

    row = await DB.fetchrow("SELECT * FROM event_completions WHERE id = ?", cid)
    return {"status": status, "points": awarded, "completion": _serialize(row)}


@app.get("/api/family/events/completions")
async def list_event_completions(device_id: str, status: Optional[str] = None):
    device = await _get_device(device_id)
    family_id = await _require_family(device)
    if status:
        rows = await DB.fetch(
            "SELECT * FROM event_completions WHERE family_id = ? AND status = ? "
            "ORDER BY completed_at DESC LIMIT 100",
            family_id, status,
        )
    else:
        rows = await DB.fetch(
            "SELECT * FROM event_completions WHERE family_id = ? ORDER BY completed_at DESC LIMIT 100",
            family_id,
        )
    return {"completions": [_serialize(r) for r in rows]}


@app.post("/api/family/events/completions/{completion_id}/approve")
async def approve_event_completion(
    completion_id: str, data: CompletionApproveReq, request: Request
):
    auth = request.headers.get("X-Device-ID", "")
    if auth != data.device_id:
        raise HTTPException(status_code=403, detail="본인 기기로만 가능")
    c = await DB.fetchrow("SELECT * FROM event_completions WHERE id = ?", completion_id)
    if not c:
        raise HTTPException(status_code=404, detail="요청을 찾을 수 없어요")
    if c["status"] != "pending":
        raise HTTPException(status_code=400, detail="이미 처리된 요청이에요")
    await _require_admin(data.device_id, c["family_id"])

    if data.action == "approve":
        points = int(c["requested_points"] or 0)
        await DB.execute(
            "UPDATE event_completions SET status = 'approved', points_awarded = ?, "
            "approved_by = ?, approved_at = ? WHERE id = ?",
            points, data.device_id, _now(), completion_id,
        )
        if points > 0:
            await _adjust_balance(c["family_id"], c["user_device_id"], points)
            await _maybe_issue_point_ticket(c["family_id"], c["user_device_id"])
        await _chat_manager.broadcast(c["family_id"], {
            "type": "event_completion",
            "status": "approved",
            "target_device_id": c["user_device_id"],
            "target_name": c["user_name"],
            "event_id": c["event_id"],
            "event_title": c["event_title"],
            "points": points,
        })
    else:
        await DB.execute(
            "UPDATE event_completions SET status = 'rejected', approved_by = ?, "
            "approved_at = ?, reject_reason = ? WHERE id = ?",
            data.device_id, _now(), data.reason, completion_id,
        )
        await _chat_manager.broadcast(c["family_id"], {
            "type": "event_rejected",
            "target_device_id": c["user_device_id"],
            "event_id": c["event_id"],
            "title": c["event_title"],
            "reason": data.reason,
        })
    return {"status": f"{data.action}d"}


@app.post("/api/family/events/reward")
async def award_event_reward(data: EventRewardReq):
    """일정 담당자에게 포인트·스티커를 지급한다.

    일정은 누구든 대신 체크해 줄 수 있어서(부모가 아이 일정을 눌러줌), 포상은
    '누른 사람'이 아니라 '해야 했던 사람'에게 가야 한다. 같은 가족 안에서만 허용.
    """
    actor = await _get_device(data.device_id)
    family_id = await _require_family(actor)

    if data.points < 0:
        raise HTTPException(status_code=400, detail="포인트는 음수일 수 없어요")

    target_device_id = await _resolve_target_device(
        family_id, data.target_user_id, data.target_device_id
    )
    target = await _get_device(target_device_id)
    if target["family_id"] != family_id:
        raise HTTPException(status_code=403, detail="같은 가족 구성원에게만 지급할 수 있어요")

    # 같은 일정으로 두 번 지급되지 않게 — 완료/취소를 반복해도 한 번만.
    dup = await DB.fetchrow(
        "SELECT id FROM member_stickers WHERE family_id = ? AND device_id = ? "
        "AND source = 'event' AND source_id = ?",
        family_id, target_device_id, data.event_id,
    )
    if dup:
        return {"status": "already_awarded", "points": 0, "completion": None}

    if data.points > 0:
        await _adjust_balance(family_id, target_device_id, data.points)

    completion = None
    if data.grant_sticker:
        completion = await _grant_sticker(
            family_id, target_device_id, target["user_name"], "event", data.event_id,
        )

    await _broadcast_sticker_update(family_id, completion)
    await _chat_manager.broadcast(family_id, {
        "type": "event_reward",
        "target_device_id": target_device_id,
        "target_name": target["user_name"],
        "actor_name": actor["user_name"],
        "points": data.points,
        "event_id": data.event_id,
        "event_title": data.event_title,
    })

    return {
        "status": "awarded",
        "points": data.points,
        "target_device_id": target_device_id,
        "target_name": target["user_name"],
        "completion": completion,
    }


# ── Phase 2: 게이미피케이션 통계 (주간 리더보드 / 스트릭) ──────────────────────────

@app.get("/api/family/stats")
async def family_stats(device_id: str):
    """가족 멤버별 통계: 주간 포인트 / 연속일(스트릭) / 총 완료수."""
    device = await _get_device(device_id)
    family_id = await _require_family(device)
    _, members = await _get_family_members(family_id)

    rows = await DB.fetch(
        "SELECT user_device_id, points_awarded, completed_at FROM mission_completions "
        "WHERE family_id = ? AND status = 'approved'",
        family_id,
    )

    today = datetime.now().date()
    week_start = today - timedelta(days=today.weekday())
    dates_by_user: Dict[str, Set] = {}
    weekly: Dict[str, int] = {}
    total: Dict[str, int] = {}
    for r in rows:
        uid = r["user_device_id"]
        dt = _as_local_dt(r["completed_at"])
        if dt is None:
            continue
        d = dt.date()
        dates_by_user.setdefault(uid, set()).add(d)
        total[uid] = total.get(uid, 0) + 1
        if d >= week_start:
            weekly[uid] = weekly.get(uid, 0) + int(r["points_awarded"] or 0)

    def _streak(dates: Set) -> int:
        if not dates:
            return 0
        start = today if today in dates else (today - timedelta(days=1))
        if start not in dates:
            return 0
        n, cur = 0, start
        while cur in dates:
            n += 1
            cur -= timedelta(days=1)
        return n

    stats = []
    for m in members:
        uid = m["device_id"]
        stats.append({
            "device_id": uid,
            "user_name": m["user_name"],
            "is_creator": m["is_creator"],
            "weekly_points": weekly.get(uid, 0),
            "streak_days": _streak(dates_by_user.get(uid, set())),
            "total_completions": total.get(uid, 0),
        })
    stats.sort(key=lambda s: (s["weekly_points"], s["total_completions"]), reverse=True)
    return {"stats": stats}


# ── 스티커판 (개인별 목표판) ────────────────────────────────────────────────────

@app.get("/api/family/sticker-board")
async def get_sticker_board(device_id: str):
    device = await _get_device(device_id)
    family_id = await _require_family(device)
    config = await _get_sticker_config(family_id)
    _, members = await _get_family_members(family_id)

    boards = []
    for m in members:
        uid = m["device_id"]
        cnt_row = await DB.fetchrow(
            "SELECT COUNT(*) as cnt FROM member_stickers WHERE family_id = ? AND device_id = ? AND consumed = 0",
            family_id, uid,
        )
        completed_row = await DB.fetchrow(
            "SELECT COUNT(*) as cnt FROM sticker_board_completions WHERE family_id = ? AND device_id = ?",
            family_id, uid,
        )
        boards.append({
            "device_id": uid,
            "user_name": m["user_name"],
            "current_count": int(cnt_row["cnt"]),
            "completed_count": int(completed_row["cnt"]),
        })

    history_rows = await DB.fetch(
        "SELECT * FROM sticker_board_completions WHERE family_id = ? ORDER BY completed_at DESC LIMIT 50",
        family_id,
    )
    return {
        "config": config,
        "boards": boards,
        "history": [_serialize(r) for r in history_rows],
    }


@app.get("/api/family/sticker-board/stickers")
async def get_member_stickers(device_id: str, target_device_id: str):
    """구성원 한 명의 스티커를 **한 알씩**, 어디서 생겼는지와 함께 돌려준다.

    스티커판은 지금까지 개수만 보여줘서 "이 포도알이 어떻게 생겼는지"를 알 수 없었다.
    한 알을 눌렀을 때 "무슨 미션으로, 언제" 받았는지 보이게 하려고 만든 목록이다.
    가족이면 서로 볼 수 있다 — 스티커판은 원래 같이 보고 축하하는 물건이다.
    """
    device = await _get_device(device_id)
    family_id = await _require_family(device)

    rows = await DB.fetch(
        "SELECT * FROM member_stickers WHERE family_id = ? AND device_id = ? AND consumed = 0 "
        "ORDER BY created_at",
        family_id, target_device_id,
    )

    # 출처 제목은 가족 단위로 한 번에 받아 매핑한다(스티커마다 조회하면 N+1).
    comp_titles = {
        r["cid"]: r["title"]
        for r in await DB.fetch(
            "SELECT mc.id AS cid, COALESCE(m.title, '미션') AS title "
            "FROM mission_completions mc LEFT JOIN missions m ON m.id = mc.mission_id "
            "WHERE mc.family_id = ?",
            family_id,
        )
    }
    event_titles = {
        r["id"]: r["title"]
        for r in await DB.fetch(
            "SELECT id, title FROM shared_events WHERE family_id = ?", family_id)
    }

    out = []
    for r in rows:
        src = r["source"] or ""
        # 미션 하나로 여러 알을 줄 때 source_id에 "#1" 같은 꼬리표가 붙는다.
        raw_id = (r["source_id"] or "").split("#")[0]
        if src == "mission":
            title = comp_titles.get(raw_id, "미션 완료")
            emoji = "🎯"
        elif src == "event":
            title = event_titles.get(raw_id, "일정 완료")
            emoji = "📅"
        elif src == "event_daily":
            # 지금은 일정 완주 보너스를 포인트로 주지만, 포도알로 주던 시절의
            # 스티커가 판에 그대로 남아 있다. "스티커"라고만 뜨면 설명이 안 된다.
            title = "하루 일정 완주"
            emoji = "🌟"
        else:
            title = "스티커"
            emoji = "🍇"
        out.append({
            "id": r["id"],
            "source": src,
            "title": title,
            "emoji": emoji,
            "created_at": r["created_at"],
        })
    return {"stickers": out}


@app.patch("/api/family/sticker-board/config")
async def update_sticker_board_config(data: StickerBoardConfigReq, request: Request):
    auth = request.headers.get("X-Device-ID", "")
    if auth != data.device_id:
        raise HTTPException(status_code=403, detail="본인 기기로만 가능")
    device = await _get_device(data.device_id)
    family_id = await _require_family(device)
    await _require_admin(data.device_id, family_id)

    if data.goal_count <= 0:
        raise HTTPException(status_code=400, detail="목표 스티커 개수는 1 이상이어야 해요")
    if not data.reward_title.strip():
        raise HTTPException(status_code=400, detail="보상 이름을 입력해주세요")

    await _get_sticker_config(family_id)  # 행이 없으면 기본값으로 생성
    await DB.execute(
        "UPDATE sticker_board_configs SET goal_count = ?, reward_title = ?, reward_description = ?, "
        "updated_by = ?, updated_at = ? WHERE family_id = ?",
        data.goal_count, data.reward_title.strip(), data.reward_description,
        data.device_id, _now(), family_id,
    )
    await _broadcast_sticker_update(family_id)
    config = await _get_sticker_config(family_id)
    return {"config": config}


# ── 공유 리스트 (장보기 / 할일 / 준비물) ──────────────────────────────────────────

async def _broadcast_list_update(family_id: str, list_id: Optional[str] = None):
    """리스트/항목 변경을 가족 전체 WS에 알림 → 클라이언트가 refresh."""
    try:
        await _chat_manager.broadcast(family_id, {"type": "list_update", "list_id": list_id})
    except Exception:
        pass


async def _list_with_counts(row: dict) -> dict:
    """리스트 요약에 항목 수/체크 수 부착."""
    d = _serialize(row)
    counts = await DB.fetchrow(
        "SELECT COUNT(*) AS total, COALESCE(SUM(is_checked), 0) AS done"
        " FROM family_list_items WHERE list_id = ?",
        row["id"],
    )
    d["item_count"] = int(counts["total"]) if counts else 0
    d["checked_count"] = int(counts["done"]) if counts else 0
    return d


@app.get("/api/family/lists")
async def list_lists(device_id: str):
    device = await _get_device(device_id)
    family_id = await _require_family(device)
    rows = await DB.fetch(
        "SELECT * FROM family_lists WHERE family_id = ? AND archived = 0 ORDER BY updated_at DESC",
        family_id,
    )
    return {"lists": [await _list_with_counts(r) for r in rows]}


@app.post("/api/family/lists")
async def create_list(data: ListUpsertReq, request: Request):
    auth = request.headers.get("X-Device-ID", "")
    if auth != data.device_id:
        raise HTTPException(status_code=403, detail="본인 기기로만 가능")
    device = await _get_device(data.device_id)
    family_id = await _require_family(device)
    if not data.title.strip():
        raise HTTPException(status_code=400, detail="리스트 이름을 입력해주세요")

    lid = str(uuid.uuid4())
    now = _now()
    await DB.execute("""
        INSERT INTO family_lists (id, family_id, created_by, title, type, icon, archived, created_at, updated_at)
        VALUES (?, ?, ?, ?, ?, ?, 0, ?, ?)
    """, lid, family_id, data.device_id, data.title.strip(), data.type, data.icon, now, now)
    await _broadcast_list_update(family_id, lid)
    row = await DB.fetchrow("SELECT * FROM family_lists WHERE id = ?", lid)
    return {"list": await _list_with_counts(row)}


@app.patch("/api/family/lists/{list_id}")
async def update_list(list_id: str, data: ListUpsertReq, request: Request):
    auth = request.headers.get("X-Device-ID", "")
    if auth != data.device_id:
        raise HTTPException(status_code=403, detail="본인 기기로만 가능")
    existing = await DB.fetchrow("SELECT * FROM family_lists WHERE id = ?", list_id)
    if not existing:
        raise HTTPException(status_code=404, detail="리스트를 찾을 수 없어요")
    await _require_same_family(data.device_id, existing["family_id"])
    if not data.title.strip():
        raise HTTPException(status_code=400, detail="리스트 이름을 입력해주세요")

    await DB.execute(
        "UPDATE family_lists SET title = ?, type = ?, icon = ?, updated_at = ? WHERE id = ?",
        data.title.strip(), data.type, data.icon, _now(), list_id,
    )
    await _broadcast_list_update(existing["family_id"], list_id)
    row = await DB.fetchrow("SELECT * FROM family_lists WHERE id = ?", list_id)
    return {"list": await _list_with_counts(row)}


@app.delete("/api/family/lists/{list_id}")
async def delete_list(list_id: str, device_id: str):
    existing = await DB.fetchrow("SELECT * FROM family_lists WHERE id = ?", list_id)
    if not existing:
        return {"status": "ok"}  # idempotent
    await _require_same_family(device_id, existing["family_id"])
    await DB.execute("UPDATE family_lists SET archived = 1, updated_at = ? WHERE id = ?", _now(), list_id)
    await _broadcast_list_update(existing["family_id"], list_id)
    return {"status": "ok"}


@app.get("/api/family/lists/{list_id}/items")
async def get_list_items(list_id: str, device_id: str):
    device = await _get_device(device_id)
    family_id = await _require_family(device)
    lst = await DB.fetchrow("SELECT * FROM family_lists WHERE id = ?", list_id)
    if not lst or lst["family_id"] != family_id:
        raise HTTPException(status_code=404, detail="리스트를 찾을 수 없어요")
    rows = await DB.fetch(
        "SELECT * FROM family_list_items WHERE list_id = ?"
        " ORDER BY is_checked ASC, position ASC, created_at ASC",
        list_id,
    )
    return {"items": [_serialize(r) for r in rows]}


@app.post("/api/family/lists/{list_id}/items")
async def add_list_item(list_id: str, data: ListItemCreateReq, request: Request):
    auth = request.headers.get("X-Device-ID", "")
    if auth != data.device_id:
        raise HTTPException(status_code=403, detail="본인 기기로만 가능")
    device = await _get_device(data.device_id)
    family_id = await _require_family(device)
    lst = await DB.fetchrow("SELECT * FROM family_lists WHERE id = ?", list_id)
    if not lst or lst["family_id"] != family_id:
        raise HTTPException(status_code=404, detail="리스트를 찾을 수 없어요")
    if not data.content.strip():
        raise HTTPException(status_code=400, detail="내용을 입력해주세요")

    maxpos = await DB.fetchval("SELECT MAX(position) FROM family_list_items WHERE list_id = ?", list_id)
    pos = (int(maxpos) + 1) if maxpos is not None else 0
    iid = str(uuid.uuid4())
    now = _now()
    await DB.execute("""
        INSERT INTO family_list_items (id, list_id, family_id, content, quantity, is_checked,
            checked_by, checked_by_name, created_by, position, created_at, updated_at)
        VALUES (?, ?, ?, ?, ?, 0, NULL, NULL, ?, ?, ?, ?)
    """, iid, list_id, family_id, data.content.strip(), data.quantity, data.device_id, pos, now, now)
    await DB.execute("UPDATE family_lists SET updated_at = ? WHERE id = ?", now, list_id)
    await _broadcast_list_update(family_id, list_id)
    row = await DB.fetchrow("SELECT * FROM family_list_items WHERE id = ?", iid)
    return {"item": _serialize(row)}


@app.patch("/api/family/lists/{list_id}/items/{item_id}")
async def patch_list_item(list_id: str, item_id: str, data: ListItemPatchReq, request: Request):
    auth = request.headers.get("X-Device-ID", "")
    if auth != data.device_id:
        raise HTTPException(status_code=403, detail="본인 기기로만 가능")
    device = await _get_device(data.device_id)
    family_id = await _require_family(device)
    item = await DB.fetchrow("SELECT * FROM family_list_items WHERE id = ?", item_id)
    if not item or item["family_id"] != family_id or item["list_id"] != list_id:
        raise HTTPException(status_code=404, detail="항목을 찾을 수 없어요")

    now = _now()
    if data.is_checked is not None:
        if data.is_checked:
            await DB.execute(
                "UPDATE family_list_items SET is_checked = 1, checked_by = ?, checked_by_name = ?, updated_at = ? WHERE id = ?",
                data.device_id, device["user_name"], now, item_id,
            )
        else:
            await DB.execute(
                "UPDATE family_list_items SET is_checked = 0, checked_by = NULL, checked_by_name = NULL, updated_at = ? WHERE id = ?",
                now, item_id,
            )
    if data.content is not None and data.content.strip():
        await DB.execute("UPDATE family_list_items SET content = ?, updated_at = ? WHERE id = ?", data.content.strip(), now, item_id)
    if data.quantity is not None:
        await DB.execute("UPDATE family_list_items SET quantity = ?, updated_at = ? WHERE id = ?", data.quantity, now, item_id)

    await DB.execute("UPDATE family_lists SET updated_at = ? WHERE id = ?", now, list_id)
    await _broadcast_list_update(family_id, list_id)
    row = await DB.fetchrow("SELECT * FROM family_list_items WHERE id = ?", item_id)
    return {"item": _serialize(row)}


@app.delete("/api/family/lists/{list_id}/items/{item_id}")
async def delete_list_item(list_id: str, item_id: str, device_id: str):
    item = await DB.fetchrow("SELECT * FROM family_list_items WHERE id = ?", item_id)
    if not item:
        return {"status": "ok"}  # idempotent
    await _require_same_family(device_id, item["family_id"])
    await DB.execute("DELETE FROM family_list_items WHERE id = ?", item_id)
    await DB.execute("UPDATE family_lists SET updated_at = ? WHERE id = ?", _now(), list_id)
    await _broadcast_list_update(item["family_id"], list_id)
    return {"status": "ok"}


# ── Phase 4: 이벤트 코멘트 (일정별 가족 대화) ───────────────────────────────────

async def _broadcast_event_comment(family_id: str, event_id: str):
    try:
        await _chat_manager.broadcast(family_id, {"type": "event_comment", "event_id": event_id})
    except Exception:
        pass


@app.get("/api/family/events/{event_id}/comments")
async def list_event_comments(event_id: str, device_id: str):
    device = await _get_device(device_id)
    family_id = await _require_family(device)
    rows = await DB.fetch(
        "SELECT * FROM event_comments WHERE family_id = ? AND event_id = ? ORDER BY created_at ASC",
        family_id, event_id,
    )
    return {"comments": [_serialize(r) for r in rows]}


@app.post("/api/family/events/{event_id}/comments")
async def add_event_comment(event_id: str, data: EventCommentReq, request: Request):
    auth = request.headers.get("X-Device-ID", "")
    if auth != data.device_id:
        raise HTTPException(status_code=403, detail="본인 기기로만 가능")
    content = data.content.strip()
    if not content:
        raise HTTPException(status_code=400, detail="댓글이 비어 있어요")
    if len(content) > 1000:
        raise HTTPException(status_code=400, detail="댓글이 너무 길어요 (최대 1000자)")
    device = await _get_device(data.device_id)
    family_id = await _require_family(device)
    cid = str(uuid.uuid4())
    await DB.execute(
        "INSERT INTO event_comments (id, event_id, family_id, device_id, user_name, content, created_at)"
        " VALUES (?, ?, ?, ?, ?, ?, ?)",
        cid, event_id, family_id, data.device_id, device["user_name"], content, _now_utc(),
    )
    await _broadcast_event_comment(family_id, event_id)
    row = await DB.fetchrow("SELECT * FROM event_comments WHERE id = ?", cid)
    return {"comment": _serialize(row)}


@app.delete("/api/family/events/{event_id}/comments/{comment_id}")
async def delete_event_comment(event_id: str, comment_id: str, device_id: str):
    row = await DB.fetchrow("SELECT * FROM event_comments WHERE id = ?", comment_id)
    if not row:
        return {"status": "ok"}
    device = await _get_device(device_id)
    family_id = await _require_family(device)
    if row["family_id"] != family_id:
        raise HTTPException(status_code=403, detail="같은 가족이 아니에요")
    family = await DB.fetchrow("SELECT created_by FROM families WHERE id = ?", family_id)
    is_admin = bool(family and family["created_by"] == device_id)
    if row["device_id"] != device_id and not is_admin:
        raise HTTPException(status_code=403, detail="본인 댓글만 삭제할 수 있어요")
    await DB.execute("DELETE FROM event_comments WHERE id = ?", comment_id)
    await _broadcast_event_comment(family_id, event_id)
    return {"status": "ok"}


# ── 공유 일정 확인(읽음) 표시 ─────────────────────────────────────────────────
#
# 가족이 일정을 공유해도 "봤는지"를 알 수 없어서, 결국 채팅으로 "일정 봤어?"를
# 다시 묻게 된다. 일정 상세를 연 가족을 기록해 등록한 사람이 바로 확인할 수 있게 한다.

async def _broadcast_event_seen(family_id: str, event_id: str):
    try:
        await _chat_manager.broadcast(family_id, {"type": "event_seen", "event_id": event_id})
    except Exception:
        pass


@app.post("/api/family/events/{event_id}/seen")
async def mark_event_seen(event_id: str, data: EventSeenReq, request: Request):
    """일정을 확인했다고 기록한다. 기기당 처음 한 번만 남는다(멱등)."""
    if request.headers.get("X-Device-ID", "") != data.device_id:
        raise HTTPException(status_code=403, detail="본인 기기로만 가능")
    device = await _get_device(data.device_id)
    family_id = await _require_family(device)
    event = await DB.fetchrow(
        "SELECT family_id, created_by FROM shared_events WHERE id = ?", event_id)
    if not event or event["family_id"] != family_id:
        raise HTTPException(status_code=404, detail="공유된 일정이 아니에요")
    # 등록한 사람은 당연히 알고 있으니 기록하지 않는다.
    if event["created_by"] == data.device_id:
        return {"status": "ok", "recorded": False}
    exists = await DB.fetchrow(
        "SELECT 1 AS x FROM event_views WHERE event_id = ? AND device_id = ?",
        event_id, data.device_id)
    if exists:
        return {"status": "ok", "recorded": False}
    await DB.execute(
        "INSERT INTO event_views (event_id, family_id, device_id, user_name, seen_at)"
        " VALUES (?, ?, ?, ?, ?) ON CONFLICT (event_id, device_id) DO NOTHING",
        event_id, family_id, data.device_id, device["user_name"], _now_utc(),
    )
    await _broadcast_event_seen(family_id, event_id)
    return {"status": "ok", "recorded": True}


@app.get("/api/family/events/{event_id}/seen")
async def get_event_seen(event_id: str, device_id: str):
    """확인한 가족과 아직 확인하지 않은 가족. 등록자는 양쪽 어디에도 넣지 않는다."""
    device = await _get_device(device_id)
    family_id = await _require_family(device)
    event = await DB.fetchrow(
        "SELECT family_id, created_by FROM shared_events WHERE id = ?", event_id)
    if not event or event["family_id"] != family_id:
        raise HTTPException(status_code=404, detail="공유된 일정이 아니에요")
    rows = await DB.fetch(
        "SELECT device_id, user_name, seen_at FROM event_views"
        " WHERE family_id = ? AND event_id = ? ORDER BY seen_at ASC",
        family_id, event_id,
    )
    _, members = await _get_family_members(family_id)
    seen_ids = {r["device_id"] for r in rows}
    # 가족을 떠난 사람의 기록은 보여주지 않는다 — 지금 구성원 기준으로만 센다.
    member_ids = {m["device_id"] for m in members}
    seen = [_serialize(r) for r in rows if r["device_id"] in member_ids]
    unseen = [
        {"device_id": m["device_id"], "user_name": m["user_name"]}
        for m in members
        if m["device_id"] != event["created_by"] and m["device_id"] not in seen_ids
    ]
    return {"created_by": event["created_by"], "seen": seen, "unseen": unseen}


# ── Phase 4: 주간 식단 플래너 (가족 공유 주간 메뉴) ──────────────────────────────

class MealUpsertReq(BaseModel):
    device_id: str
    date: str            # 'YYYY-MM-DD'
    meal_type: str = "dinner"   # breakfast|lunch|dinner|snack
    title: str
    memo: Optional[str] = None


class MealPatchReq(BaseModel):
    device_id: str
    date: Optional[str] = None
    meal_type: Optional[str] = None
    title: Optional[str] = None
    memo: Optional[str] = None


async def _broadcast_meal_update(family_id: str):
    try:
        await _chat_manager.broadcast(family_id, {"type": "meal_update"})
    except Exception:
        pass


@app.get("/api/family/meals")
async def list_meals(device_id: str, start: Optional[str] = None, end: Optional[str] = None):
    device = await _get_device(device_id)
    family_id = await _require_family(device)
    if start and end:
        rows = await DB.fetch(
            "SELECT * FROM meal_plans WHERE family_id = ? AND date >= ? AND date <= ?"
            " ORDER BY date ASC, created_at ASC",
            family_id, start, end,
        )
    else:
        rows = await DB.fetch(
            "SELECT * FROM meal_plans WHERE family_id = ? ORDER BY date ASC, created_at ASC LIMIT 200",
            family_id,
        )
    return {"meals": [_serialize(r) for r in rows]}


@app.post("/api/family/meals")
async def create_meal(data: MealUpsertReq, request: Request):
    auth = request.headers.get("X-Device-ID", "")
    if auth != data.device_id:
        raise HTTPException(status_code=403, detail="본인 기기로만 가능")
    device = await _get_device(data.device_id)
    family_id = await _require_family(device)
    title = data.title.strip()
    if not title:
        raise HTTPException(status_code=400, detail="메뉴를 입력해주세요")
    if not data.date:
        raise HTTPException(status_code=400, detail="날짜가 필요해요")
    if data.meal_type not in {"breakfast", "lunch", "dinner", "snack"}:
        raise HTTPException(status_code=400, detail="잘못된 식사 종류예요")
    mid = str(uuid.uuid4())
    now = _now()
    await DB.execute(
        "INSERT INTO meal_plans (id, family_id, date, meal_type, title, memo, created_by, created_at, updated_at)"
        " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
        mid, family_id, data.date, data.meal_type, title, data.memo, data.device_id, now, now,
    )
    await _broadcast_meal_update(family_id)
    row = await DB.fetchrow("SELECT * FROM meal_plans WHERE id = ?", mid)
    return {"meal": _serialize(row)}


@app.patch("/api/family/meals/{meal_id}")
async def update_meal(meal_id: str, data: MealPatchReq, request: Request):
    auth = request.headers.get("X-Device-ID", "")
    if auth != data.device_id:
        raise HTTPException(status_code=403, detail="본인 기기로만 가능")
    existing = await DB.fetchrow("SELECT * FROM meal_plans WHERE id = ?", meal_id)
    if not existing:
        raise HTTPException(status_code=404, detail="식단을 찾을 수 없어요")
    device = await _get_device(data.device_id)
    family_id = await _require_family(device)
    if existing["family_id"] != family_id:
        raise HTTPException(status_code=403, detail="같은 가족이 아니에요")
    now = _now()
    if data.title is not None and data.title.strip():
        await DB.execute("UPDATE meal_plans SET title = ?, updated_at = ? WHERE id = ?", data.title.strip(), now, meal_id)
    if data.memo is not None:
        await DB.execute("UPDATE meal_plans SET memo = ?, updated_at = ? WHERE id = ?", data.memo, now, meal_id)
    if data.date is not None and data.date:
        await DB.execute("UPDATE meal_plans SET date = ?, updated_at = ? WHERE id = ?", data.date, now, meal_id)
    if data.meal_type is not None and data.meal_type:
        await DB.execute("UPDATE meal_plans SET meal_type = ?, updated_at = ? WHERE id = ?", data.meal_type, now, meal_id)
    await _broadcast_meal_update(family_id)
    row = await DB.fetchrow("SELECT * FROM meal_plans WHERE id = ?", meal_id)
    return {"meal": _serialize(row)}


@app.delete("/api/family/meals/{meal_id}")
async def delete_meal(meal_id: str, device_id: str):
    row = await DB.fetchrow("SELECT * FROM meal_plans WHERE id = ?", meal_id)
    if not row:
        return {"status": "ok"}
    device = await _get_device(device_id)
    family_id = await _require_family(device)
    if row["family_id"] != family_id:
        raise HTTPException(status_code=403, detail="같은 가족이 아니에요")
    await DB.execute("DELETE FROM meal_plans WHERE id = ?", meal_id)
    await _broadcast_meal_update(family_id)
    return {"status": "ok"}


# ── 지혜섬 탐험대: 수학·국어·영어·과학 (/learn/) ───────────────────────────────────────────────
# 학습 웹앱(github.com/limchaekuk-zzz0903/Elementaryschoolworksheets)을 같은 출처에서
# 서빙한다. 웹앱은 /learn/ 에서 열리면 상대경로 api/progress 로 진도를 주고받는다
# (GET → {name, progress}, PUT ← {progress, reset}). 파일 갱신: server/learn/update.sh
# catalog.json(과목·화 목록)으로 홈 카드의 과목별 진도와 과목 바로 가기를 만든다.
#
# 인증: 앱 웹뷰가 /learn/ 첫 요청에만 X-Device 헤더를 싣는다 → 여기서 확인하고
# learn_sessions 토큰을 HttpOnly 쿠키로 준다. 이후 새로고침·api 호출은 쿠키로.
# 헤더도 쿠키도 없으면(일반 브라우저) 페이지는 그대로 열리고 진도는 기기에만 남는다.

_LEARN_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "learn")
_LEARN_FILES = {
    "index.html": "text/html; charset=utf-8",
    "catalog.json": "application/json",
    "manifest.webmanifest": "application/manifest+json",
    "sw.js": "text/javascript; charset=utf-8",
    "icon.svg": "image/svg+xml",
    "icon-192.png": "image/png",
    "icon-512.png": "image/png",
}
_LEARN_COOKIE = "toh_learn"
_LEARN_SESSION_DAYS = 30
_LEARN_MAX_BODY = 512 * 1024
_learn_cache: Dict[str, bytes] = {}


def _learn_file(name: str) -> bytes:
    if name not in _learn_cache:
        with open(os.path.join(_LEARN_DIR, name), "rb") as f:
            data = f.read()
        if name == "sw.js":
            # 배포마다 캐시 이름이 바뀌어야 설치된 웹앱이 새 버전을 받는다.
            try:
                with open(os.path.join(_LEARN_DIR, "VERSION")) as vf:
                    build = vf.read().strip()[:12] or "local"
            except OSError:
                build = hashlib.sha256(_learn_file("index.html")).hexdigest()[:12]
            data = data.replace(b"__BUILD__", build.encode())
        _learn_cache[name] = data
    return _learn_cache[name]


def _secret_digest(secret: str) -> str:
    return hashlib.sha256(secret.encode("utf-8")).hexdigest()


def _is_https(request: Request) -> bool:
    proto = request.headers.get("x-forwarded-proto", request.url.scheme)
    return proto.split(",")[0].strip() == "https"


async def _learn_issue_session(request: Request) -> Optional[str]:
    """X-Device 헤더가 맞으면 새 학습 세션 토큰을 만든다. 계정 없는 기기는 None."""
    device_id = request.headers.get("X-Device-ID", "")
    secret = request.headers.get("X-Device-Secret", "")
    if not device_id or not secret:
        return None
    row = await DB.fetchrow(
        "SELECT secret, user_id FROM device_secrets WHERE device_id = ?", device_id
    )
    if not row or row["secret"] != secret or not row.get("user_id"):
        return None
    now = int(datetime.now(timezone.utc).timestamp())
    # 같은 기기의 옛 토큰과 만료된 토큰은 정리 — 표가 끝없이 자라지 않게.
    await DB.execute(
        "DELETE FROM learn_sessions WHERE device_id = ? OR expires_at < ?", device_id, now
    )
    # 뒤에 비밀의 지문을 붙여, 비밀이 바뀌면(다시 로그인) 이 토큰도 죽게 한다.
    token = f"{uuid.uuid4().hex}{uuid.uuid4().hex}.{_secret_digest(secret)[:16]}"
    await DB.execute(
        "INSERT INTO learn_sessions (token, device_id, user_id, expires_at) VALUES (?, ?, ?, ?)",
        token, device_id, row["user_id"], now + _LEARN_SESSION_DAYS * 86400,
    )
    return token


async def _learn_session_user(request: Request) -> Optional[dict]:
    """쿠키의 학습 세션 → {user_id, device_id}. 만료·재로그인·기기 탈퇴면 None."""
    token = request.cookies.get(_LEARN_COOKIE, "")
    if not token or "." not in token:
        return None
    row = await DB.fetchrow(
        "SELECT ls.device_id, ls.user_id, ls.expires_at, ds.secret, ds.user_id AS cur_user "
        "FROM learn_sessions ls JOIN device_secrets ds ON ds.device_id = ls.device_id "
        "WHERE ls.token = ?",
        token,
    )
    if not row:
        return None
    if int(row["expires_at"]) < int(datetime.now(timezone.utc).timestamp()):
        return None
    if row["cur_user"] != row["user_id"]:
        return None
    if token.rsplit(".", 1)[1] != _secret_digest(row["secret"])[:16]:
        return None
    return {"user_id": row["user_id"], "device_id": row["device_id"]}


async def _learn_user_name(user_id: str, device_id: Optional[str] = None) -> str:
    u = await DB.fetchrow("SELECT name FROM users WHERE id = ?", user_id)
    if u and u.get("name"):
        return u["name"]
    if device_id:
        d = await DB.fetchrow("SELECT user_name FROM devices WHERE id = ?", device_id)
        if d and d.get("user_name"):
            return d["user_name"]
    return ""


async def _learn_load(user_id: str) -> dict:
    row = await DB.fetchrow("SELECT progress FROM learn_progress WHERE user_id = ?", user_id)
    if not row:
        return {}
    try:
        v = json.loads(row["progress"])
        return v if isinstance(v, dict) else {}
    except Exception:
        return {}


def _learn_clean_stars(v) -> Dict[str, int]:
    out: Dict[str, int] = {}
    if isinstance(v, dict):
        for k, s in v.items():
            if isinstance(k, str) and len(k) <= 64:
                try:
                    out[k] = max(0, min(3, int(s)))
                except (TypeError, ValueError):
                    pass
    return out


def _learn_clean_crystals(v) -> Dict[str, bool]:
    if not isinstance(v, dict):
        return {}
    return {k: True for k, c in v.items() if isinstance(k, str) and len(k) <= 64 and c}


_LEARN_COUNTERS = ("xp", "maxCombo", "correct", "cleared")
_LEARN_DATED = ("badges", "medals")  # 획득한 것 → 받은 날짜. 한 번 받으면 사라지지 않는다.


def _learn_int(v, hi: int = 10**9) -> int:
    try:
        return max(0, min(hi, int(v)))
    except (TypeError, ValueError):
        return 0


def _learn_clean_dated(v) -> Dict[str, str]:
    if not isinstance(v, dict):
        return {}
    return {k: str(d)[:10] for k, d in list(v.items())[:500]
            if isinstance(k, str) and len(k) <= 64 and d}


def _learn_clean_scores(v) -> Dict[str, int]:
    """오늘의 도전(날짜 → 점수)·스피드 도전(과목-학년 → 최고 기록)."""
    if not isinstance(v, dict):
        return {}
    return {k: _learn_int(s, 10**6) for k, s in list(v.items())[-500:]
            if isinstance(k, str) and len(k) <= 64}


def _learn_clean_days(v) -> List[str]:
    if not isinstance(v, list):
        return []
    return sorted({d[:10] for d in v if isinstance(d, str) and len(d) >= 10})[-400:]


def _learn_merge(old: dict, new: dict, reset: bool) -> dict:
    """두 기기에서 따로 풀어도 줄어들지 않게 합친다: 별·카운터·기록은 큰 쪽,
    크리스탈·배지·메달·공부한 날은 합집합. 오답 노트와 학습 로그는 웹앱이 기기에만
    두므로(서버엔 안 보냄) 예전 버전이 남긴 것도 여기서 버린다.
    reset(웹앱의 '처음부터 다시')이면 서버 기록도 새 값으로 갈아엎는다."""
    merged = {
        "name": str(new.get("name") or old.get("name") or "")[:20],
        "stars": _learn_clean_stars(new.get("stars")),
        "crystals": _learn_clean_crystals(new.get("crystals")),
        "badges": _learn_clean_dated(new.get("badges")),
        "medals": _learn_clean_dated(new.get("medals")),
        "daily": _learn_clean_scores(new.get("daily")),
        "speed": _learn_clean_scores(new.get("speed")),
        "days": _learn_clean_days(new.get("days")),
        **{k: _learn_int(new.get(k)) for k in _LEARN_COUNTERS},
    }
    if reset:
        return merged
    for k, s in _learn_clean_stars(old.get("stars")).items():
        merged["stars"][k] = max(merged["stars"].get(k, 0), s)
    merged["crystals"].update(_learn_clean_crystals(old.get("crystals")))
    for key in _LEARN_DATED:
        merged[key] = {**merged[key], **_learn_clean_dated(old.get(key))}  # 먼저 받은 날짜 유지
    for key in ("daily", "speed"):
        for k, s in _learn_clean_scores(old.get(key)).items():
            merged[key][k] = max(merged[key].get(k, 0), s)
    merged["days"] = _learn_clean_days(merged["days"] + _learn_clean_days(old.get("days")))
    for k in _LEARN_COUNTERS:
        merged[k] = max(merged[k], _learn_int(old.get(k)))
    return merged


def _learn_level(xp: int) -> int:
    """웹앱과 같은 식: xpFor(n)=20n(n+1), 레벨은 xp < xpFor(n)인 가장 작은 n."""
    n = 1
    while xp >= 20 * n * (n + 1):
        n += 1
    return n


_learn_catalog_cache: Optional[dict] = None


def _learn_catalog() -> dict:
    """catalog.json(과목 → 학년 → 지역 → 화)과 화·크리스탈 → 과목 색인."""
    global _learn_catalog_cache
    if _learn_catalog_cache is None:
        try:
            cat = json.loads(_learn_file("catalog.json"))
        except Exception:
            cat = {"subjects": []}
        lesson_subj: Dict[str, str] = {}
        region_subj: Dict[str, str] = {}
        for s in cat.get("subjects", []):
            for g in s.get("grades", []):
                for r in g.get("regions", []):
                    region_subj[r["id"]] = s["id"]
                    for l in r.get("lessons", []):
                        lesson_subj[l["id"]] = s["id"]
        _learn_catalog_cache = {"cat": cat, "lesson": lesson_subj, "region": region_subj}
    return _learn_catalog_cache


@app.get("/learn", include_in_schema=False)
async def learn_redirect():
    # 상대경로 api/progress 가 /learn/api/progress 로 풀리려면 끝 슬래시가 필요하다.
    return Response(status_code=308, headers={"Location": "/learn/"})


@app.get("/learn/", include_in_schema=False)
async def learn_index(request: Request):
    resp = Response(
        content=_learn_file("index.html"),
        media_type=_LEARN_FILES["index.html"],
        headers={"Cache-Control": "no-cache"},
    )
    token = await _learn_issue_session(request)
    if token:
        resp.set_cookie(
            _LEARN_COOKIE, token,
            max_age=_LEARN_SESSION_DAYS * 86400, path="/learn",
            httponly=True, samesite="lax", secure=_is_https(request),
        )
    return resp


@app.get("/learn/api/progress", include_in_schema=False)
async def learn_get_progress(request: Request):
    who = await _learn_session_user(request)
    if not who:
        return JSONResponse({"detail": "로그인이 필요해요"}, status_code=401)
    progress = await _learn_load(who["user_id"])
    name = await _learn_user_name(who["user_id"], who["device_id"])
    return {"name": name, "progress": progress}


@app.put("/learn/api/progress", include_in_schema=False)
async def learn_put_progress(request: Request):
    who = await _learn_session_user(request)
    if not who:
        return JSONResponse({"detail": "로그인이 필요해요"}, status_code=401)
    raw = await request.body()
    if len(raw) > _LEARN_MAX_BODY:
        return JSONResponse({"detail": "진도 데이터가 너무 커요"}, status_code=413)
    try:
        body = json.loads(raw or b"{}")
    except Exception:
        return JSONResponse({"detail": "JSON 형식이 아니에요"}, status_code=400)
    new = body.get("progress") if isinstance(body, dict) else None
    if not isinstance(new, dict):
        return JSONResponse({"detail": "progress가 필요해요"}, status_code=400)
    merged = _learn_merge(await _learn_load(who["user_id"]), new, bool(body.get("reset")))
    await DB.execute(
        "INSERT INTO learn_progress (user_id, progress, updated_at) VALUES (?, ?, ?) "
        "ON CONFLICT (user_id) DO UPDATE SET progress = excluded.progress, updated_at = excluded.updated_at",
        who["user_id"], json.dumps(merged, ensure_ascii=False), _now_utc(),
    )
    return {"status": "ok"}


import learn_tts  # noqa: E402 — 읽어 주기 음성(/learn/api/tts…). 로그인 없이 누구나.

learn_tts.register(app)


@app.get("/learn/{name}", include_in_schema=False)
async def learn_static(name: str):
    if name not in _LEARN_FILES:
        raise HTTPException(status_code=404, detail="없는 파일이에요")
    return Response(
        content=_learn_file(name),
        media_type=_LEARN_FILES[name],
        # 서비스 워커는 매번 새로 확인해야 새 배포를 알아챈다.
        headers={"Cache-Control": "no-cache" if name in {"sw.js", "manifest.webmanifest"} else "public, max-age=86400"},
    )


@app.get("/api/learn/family")
async def learn_family_summary(request: Request):
    """홈 카드용 — 과목 목록(바로 가기)과 가족 각자의 레벨·배지·과목별 진도."""
    catalog = _learn_catalog()
    subjects = [
        {"id": s["id"], "name": s["name"], "icon": s.get("icon", ""), "span": s.get("span", ""),
         "start": s.get("start", s["id"]), "lessons_total": s.get("lessons_total", 0),
         "crystals_total": s.get("crystals_total", 0)}
        for s in catalog["cat"].get("subjects", [])
    ]
    device = await _get_device(request.headers.get("X-Device-ID", ""))
    if not device.get("family_id"):
        members = [{"device_id": device["id"], "user_name": device["user_name"],
                    "family_role": "member",
                    "user_id": await DB.fetchval(
                        "SELECT user_id FROM device_secrets WHERE device_id = ?", device["id"])}]
    else:
        _, members = await _get_family_members(device["family_id"])
    out = []
    for m in members:
        if not m.get("user_id"):
            continue
        p = await _learn_load(m["user_id"])
        stars = _learn_clean_stars(p.get("stars"))
        crystals = _learn_clean_crystals(p.get("crystals"))
        days = _learn_clean_days(p.get("days"))
        if not days and isinstance(p.get("log"), list):  # 예전 버전이 남긴 학습 로그
            days = _learn_clean_days([e.get("d") for e in p["log"] if isinstance(e, dict)])
        per: Dict[str, dict] = {s["id"]: {"stars": 0, "lessons": 0, "crystals": 0} for s in subjects}
        for k, n in stars.items():
            sid = catalog["lesson"].get(k)
            if sid in per and n > 0:
                per[sid]["stars"] += n
                per[sid]["lessons"] += 1
        for k in crystals:
            sid = catalog["region"].get(k)
            if sid in per:
                per[sid]["crystals"] += 1
        xp = _learn_int(p.get("xp"))
        out.append({
            "user_id": m["user_id"],
            "name": m["user_name"],
            "family_role": m["family_role"],
            "is_me": m["device_id"] == device["id"],
            "stars": sum(stars.values()),
            "crystals": len(crystals),
            "lessons": sum(1 for n in stars.values() if n > 0),
            "xp": xp,
            "level": _learn_level(xp),
            "badges": len(_learn_clean_dated(p.get("badges"))),
            "subjects": [{"id": sid, **v} for sid, v in per.items() if v["lessons"] or v["crystals"]],
            "last_studied": days[-1] if days else None,
        })
    return {"subjects": subjects, "badges_total": catalog["cat"].get("badges_total", 0), "members": out}


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
