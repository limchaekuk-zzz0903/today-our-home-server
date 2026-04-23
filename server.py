import os
import sys

print(f"Python: {sys.version}", flush=True)
print(f"PORT env: {os.environ.get('PORT', 'NOT SET')}", flush=True)

import uvicorn

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8000))
    print(f"Starting uvicorn on 0.0.0.0:{port}", flush=True)
    # main:app 로드 실패 시 test_app으로 fallback
    try:
        import main  # noqa: F401
        uvicorn.run("main:app", host="0.0.0.0", port=port, log_level="info")
    except Exception as e:
        print(f"main:app 로드 실패: {e} — test_app으로 fallback", flush=True)
        uvicorn.run("test_app:app", host="0.0.0.0", port=port, log_level="info")
