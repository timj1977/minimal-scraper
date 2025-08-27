# run_uvicorn.py
import sys, asyncio

# Ensure Proactor loop is used in BOTH parent & reloader child on Windows
if sys.platform.startswith("win"):
    try:
        asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())
    except Exception:
        pass

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        "app.main:app",
        host="127.0.0.1",
        port=8002,
        reload=True,           # dev mode; reloader inherits the policy now
        # loop="asyncio",      # default; explicit is fine too
    )
