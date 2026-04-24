from __future__ import annotations

import json
import os
import sys

import requests


BASE_URL = "http://127.0.0.1:8000"


def _configure_console_utf8() -> None:
    os.environ.setdefault("PYTHONUTF8", "1")
    for stream_name in ("stdin", "stdout", "stderr"):
        stream = getattr(sys, stream_name, None)
        reconfigure = getattr(stream, "reconfigure", None)
        if callable(reconfigure):
            reconfigure(encoding="utf-8", errors="replace")

    if os.name != "nt":
        return

    try:
        import ctypes

        kernel32 = ctypes.windll.kernel32
        kernel32.SetConsoleCP(65001)
        kernel32.SetConsoleOutputCP(65001)
    except Exception:
        pass


def _post_chat_message(session: requests.Session, user_text: str) -> dict:
    payload = json.dumps({"message": user_text}, ensure_ascii=False).encode("utf-8")
    response = session.post(
        f"{BASE_URL}/interaction/chat",
        data=payload,
        headers={
            "Content-Type": "application/json; charset=utf-8",
            "Accept": "application/json; charset=utf-8",
        },
        timeout=90,
    )
    response.raise_for_status()
    response.encoding = "utf-8"
    return json.loads(response.content.decode("utf-8"))


def main() -> None:
    _configure_console_utf8()
    print("已连接 Interaction Agent 聊天模式。输入 exit 退出。")
    session = requests.Session()
    while True:
        user_text = input("你: ").strip()
        if not user_text:
            continue
        if user_text.lower() in {"exit", "quit", "q"}:
            print("已退出。")
            break

        try:
            data = _post_chat_message(session, user_text)
        except Exception as exc:
            print(f"Agent: 请求失败 -> {exc}")
            continue

        reply = data.get("reply", "(无回复)")
        action = data.get("action", "unknown")
        print(f"Agent[{action}]: {reply}")


if __name__ == "__main__":
    main()
