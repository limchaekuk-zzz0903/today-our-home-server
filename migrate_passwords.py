"""레거시 SHA-256 비밀번호 해시를 bcrypt-wrap으로 일괄 마이그레이션.

raw 비밀번호를 알 수 없으므로 bcrypt(sha256_hex)로 감싸 솔트를 추가한다.
이후 사용자가 정상 로그인하면 main.py의 rehash-on-login이 raw bcrypt로 교체한다.

실행:
  cd server
  python3 migrate_passwords.py            # 드라이런 (기본)
  python3 migrate_passwords.py --apply    # 실제 적용

DB 선택은 main.py와 동일: DATABASE_URL이 있으면 PostgreSQL, 없으면 로컬 SQLite.
"""
import argparse
import asyncio
import re

import bcrypt

from main import DB, _USE_PG, init_db  # noqa: F401


_HEX_RE = re.compile(r"^[0-9a-f]{64}$", re.IGNORECASE)


def _is_legacy_sha256(h: str) -> bool:
    return bool(h) and not h.startswith("$2") and bool(_HEX_RE.match(h))


async def run(apply: bool):
    await init_db()
    rows = await DB.fetch(
        "SELECT id, email, password_hash FROM users WHERE password_hash IS NOT NULL"
    )

    legacy = 0
    bcrypt_ok = 0
    unknown = 0
    upgraded = 0

    for r in rows:
        h = r.get("password_hash") or ""
        if h.startswith("$2"):
            bcrypt_ok += 1
            continue
        if not _is_legacy_sha256(h):
            unknown += 1
            print(f"  ⚠️  알 수 없는 해시 형식: email={r.get('email')!r} len={len(h)}")
            continue

        legacy += 1
        if apply:
            new_h = bcrypt.hashpw(h.encode("ascii"), bcrypt.gensalt()).decode("ascii")
            await DB.execute(
                "UPDATE users SET password_hash = ? WHERE id = ?",
                new_h, r["id"],
            )
            upgraded += 1
            print(f"  ✓ {r.get('email')} → bcrypt-wrap")
        else:
            print(f"  [dry-run] {r.get('email')} → bcrypt-wrap 대상")

    print()
    print(f"DB                : {'PostgreSQL' if _USE_PG else 'SQLite'}")
    print(f"총 사용자(해시 보유): {len(rows)}")
    print(f"  bcrypt 이미 OK   : {bcrypt_ok}")
    print(f"  레거시 SHA-256   : {legacy}")
    print(f"  알 수 없는 형식  : {unknown}")
    if apply:
        print(f"  ✅ 마이그레이션 완료: {upgraded}")
    elif legacy > 0:
        print(f"\n💡 실제 적용하려면 --apply 플래그를 추가하세요.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--apply", action="store_true", help="실제 DB 업데이트 수행")
    args = parser.parse_args()
    asyncio.run(run(apply=args.apply))
