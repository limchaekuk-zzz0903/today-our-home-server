"""학습 페이지(/learn/) 연동 테스트.

가장 중요한 규칙: 학습 페이지에는 회원·로그인 개념이 없다. 어떤 경우에도 /learn/ 이
"회원만 볼 수 있음" 화면이나 4xx 로 막히면 안 된다. 쿠키는 진도 저장에만 쓴다.

실행: pip install -r requirements.txt pytest httpx && pytest -q
"""
import json
import os
import sys
import tempfile
import uuid

import pytest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)
os.environ.pop("DATABASE_URL", None)
os.environ["SQLITE_PATH"] = os.path.join(tempfile.mkdtemp(), "test.db")

from fastapi.testclient import TestClient  # noqa: E402

import main  # noqa: E402

BLOCK_WORDS = ("회원 전용", "앱에서 열어 주세요", "로그인이 필요")


def assert_learning_page(r):
    assert r.status_code == 200, r.status_code
    assert "text/html" in r.headers["content-type"]
    assert "지혜섬 탐험대" in r.text
    for w in BLOCK_WORDS:
        assert w not in r.text


@pytest.fixture()
def client():
    with TestClient(main.app) as c:
        yield c


@pytest.fixture()
def member(client):
    dev = "dev-" + uuid.uuid4().hex[:8]
    r = client.post("/api/auth/email/register", json={
        "email": f"{dev}@example.com", "password": "pw123456", "name": "회은", "device_id": dev})
    assert r.status_code == 200
    return {"X-Device-ID": dev, "X-Device-Secret": r.json()["token"]}


def path_of(url):
    """http://host/learn/?ticket=... → /learn/?ticket=..."""
    return "/" + url.split("://", 1)[1].split("/", 1)[1]


# ── 누구나 열 수 있어야 한다 ────────────────────────────────────────────────

def test_page_opens_without_ticket_or_cookie(client):
    assert_learning_page(client.get("/learn/"))


def test_learn_without_slash_opens(client):
    assert_learning_page(client.get("/learn"))


@pytest.mark.parametrize("ticket", ["", "bogus", "x" * 80])
def test_unknown_ticket_still_opens_page(client, ticket):
    r = client.get(f"/learn/?ticket={ticket}", follow_redirects=False)
    assert r.status_code == 303 and r.headers["location"].startswith("/learn/")
    assert "set-cookie" not in r.headers
    assert_learning_page(client.get(r.headers["location"]))


def test_reused_ticket_still_opens_page(client, member):
    url = client.post("/api/learning/session?start=kor", headers=member).json()["url"]
    first = client.get(path_of(url), follow_redirects=False)
    assert first.status_code == 303 and first.headers["location"] == "/learn/#kor"
    client.cookies.clear()
    # WebView가 같은 주소를 다시 열어도(새로고침·앱 복귀·미리 불러오기) 막히지 않는다
    again = client.get(path_of(url), follow_redirects=False)
    assert again.status_code == 303 and again.headers["location"] == "/learn/#kor"
    assert_learning_page(client.get("/learn/"))


def test_start_query_without_ticket_goes_to_screen(client):
    r = client.get("/learn/?start=eng", follow_redirects=False)
    assert r.status_code == 303 and r.headers["location"] == "/learn/#eng"
    r = client.get("/learn/?start=<script>", follow_redirects=False)
    assert_learning_page(r)


@pytest.mark.parametrize("name", ["index.html", "sw.js", "manifest.webmanifest", "icon.svg", "catalog.json"])
def test_static_files_open_without_login(client, name):
    assert client.get(f"/learn/{name}").status_code == 200


# ── 회원은 진도가 서버에 저장된다 ───────────────────────────────────────────

def test_member_progress_is_saved(client, member):
    url = client.post("/api/learning/session?start=hangul-1", headers=member).json()["url"]
    r = client.get(path_of(url), follow_redirects=False)
    assert "toh_learn=" in r.headers.get("set-cookie", "")
    body = {"progress": {"stars": {"hangul-1": 3}, "crystals": {"hangul": True}, "xp": 120}}
    assert client.put("/learn/api/progress", content=json.dumps(body)).status_code == 200
    mine = client.get("/api/learning/progress", headers=member).json()
    assert mine["subjects"]["kor"]["lessons_done"] == 1 and mine["level"] == 3


def test_progress_api_without_cookie_is_401_but_page_is_fine(client):
    client.cookies.clear()
    assert client.get("/learn/api/progress").status_code == 401
    assert_learning_page(client.get("/learn/"))


def test_session_works_even_without_devices_row(client, member):
    # devices 행이 없어도(인증 비밀값은 있음) 입장권 발급이 실패하지 않아야 한다
    with TestClient(main.app) as c:
        c.portal.call(main.DB.execute, "DELETE FROM devices WHERE id = ?", member["X-Device-ID"])
        r = c.post("/api/learning/session", headers=member)
        assert r.status_code == 200 and "/learn/?ticket=" in r.json()["url"]
        assert r.json()["public_url"].endswith("/learn/")


def test_subjects_api(client, member):
    j = client.get("/api/learning/subjects", headers=member).json()
    assert [s["id"] for s in j["subjects"]] == ["math", "kor", "eng", "sci"]
