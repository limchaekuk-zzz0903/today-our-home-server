"""읽어 주기 음성(TTS) 점검: 엔진이 실제로 음성을 만드는지, 운영 서버가 음성을 내주는지 확인한다.

사용: python scripts/diagnose_tts.py [운영 서버 주소 ...]
GitHub Actions(.github/workflows/diagnose.yml)에서 인터넷이 열린 환경으로 실행한다.
"""
import asyncio
import json
import sys
import time
import urllib.error
import urllib.parse
import urllib.request


async def engine_check():
    import edge_tts
    for voice, text in (("ko-KR-SunHiNeural", "안녕하세요. 소리 점검이에요."), ("en-US-AnaNeural", "Hello!")):
        t0 = time.time()
        try:
            out = bytearray()
            async for c in edge_tts.Communicate(text, voice, rate="-8%").stream():
                if c.get("type") == "audio":
                    out.extend(c["data"])
            print(f"ENGINE {voice}: OK {len(out)} bytes head={bytes(out[:4]).hex()} {time.time()-t0:.1f}s")
        except Exception as e:
            print(f"ENGINE {voice}: FAIL {type(e).__name__}: {e}")


def get(url, headers=None, timeout=30):
    req = urllib.request.Request(url, headers={"User-Agent": "toh-diagnose", **(headers or {})})
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return r.status, dict(r.headers), r.read()
    except urllib.error.HTTPError as e:
        return e.code, dict(e.headers), e.read()
    except Exception as e:
        return None, {}, f"{type(e).__name__}: {e}".encode()


def server_check(base):
    base = base.rstrip("/")
    st, _, body = get(base + "/api/ping", timeout=15)
    print(f"\nSERVER {base}\n  /api/ping -> {st} {body[:80]!r}")
    if st is None:
        return
    st, h, body = get(base + "/learn/")
    print(f"  /learn/ -> {st} {h.get('Content-Type')} contains-app={'지혜섬' in body.decode('utf-8', 'ignore')}")
    st, _, body = get(base + "/learn/api/tts/status")
    print(f"  /learn/api/tts/status -> {st} {body[:400].decode('utf-8', 'ignore')}")
    st, _, body = get(base + "/learn/api/tts/selftest", timeout=40)
    print(f"  /learn/api/tts/selftest -> {st} {body[:400].decode('utf-8', 'ignore')}")
    q = "/learn/api/tts?" + urllib.parse.urlencode({"t": "드디어 분수 성!", "l": "ko-KR"})
    st, h, body = get(base + q, timeout=40)
    print(f"  tts full -> {st} type={h.get('Content-Type')} len={len(body)} head={body[:4].hex()} ranges={h.get('Accept-Ranges')}")
    st, h, body = get(base + q, {"Range": "bytes=0-1"}, timeout=40)
    print(f"  tts Range 0-1 -> {st} Content-Range={h.get('Content-Range')} len={len(body)}")
    st, _, body = get(base + "/learn/sw.js")
    print(f"  sw.js bypasses /api/ -> {b'/api/' in body}")


if __name__ == "__main__":
    asyncio.run(engine_check())
    for b in sys.argv[1:]:
        if b:
            server_check(b)
