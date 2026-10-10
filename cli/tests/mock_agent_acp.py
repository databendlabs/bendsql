# Copyright 2021 Datafuse Labs
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Mock ACP v1 adapter for agent_repl.py; no model calls or client-side tools."""

import json
import os
import subprocess
import sys
import time


SESSION = "mock-session"


def record(phase, **fields):
    with open(os.environ["ACP_TEST_RECORD"], "a") as file:
        file.write(json.dumps({"phase": phase, "pid": os.getpid(), "cwd": os.getcwd(), **fields}) + "\n")


def send(value):
    print(json.dumps({"jsonrpc": "2.0", **value}), flush=True)


def update(kind, **fields):
    send({"method": "session/update", "params": {
        "sessionId": SESSION, "update": {"sessionUpdate": kind, **fields},
    }})


def answer(text):
    update("agent_message_chunk", content={"type": "text", "text": text})


def result(request_id, reason="end_turn"):
    send({"id": request_id, "result": {"stopReason": reason}})


def receive_reply(expected_id):
    while True:
        line = sys.stdin.readline()
        if not line:
            return
        message = json.loads(line)
        if message.get("id") == expected_id and "method" not in message:
            record("client_reply", reply=message)
            return
        if message.get("method") == "session/cancel":
            record("cancel", params=message["params"])


def main():
    sys.stderr.write("adapter diagnostics " * 500)
    sys.stderr.flush()
    prompt_id = None
    child = None
    for line in sys.stdin:
        request = json.loads(line)
        method = request.get("method")
        params = request.get("params", {})
        if method == "initialize":
            record("initialize", params=params, env=dict(os.environ), argv=sys.argv[1:])
            if os.environ.get("ACP_TEST_HANG_INIT"):
                time.sleep(120)
            send({"id": request["id"], "result": {
                "protocolVersion": 99 if os.environ.get("ACP_TEST_BAD_VERSION") else 1,
                "agentCapabilities": {"loadSession": False}, "authMethods": [],
                "agentInfo": {"name": "mock-adapter", "version": "1"},
            }})
        elif method == "session/new":
            record("new_session", params=params)
            send({"id": request["id"], "result": {"sessionId": SESSION}})
        elif method == "session/prompt":
            prompt_id = request["id"]
            messages = json.loads(params["prompt"][0]["text"].split("\n", 1)[1])
            question = json.loads(messages[-1]["content"])["question"]
            if question in ["block-acp", "ignore-cancel"]:
                child = subprocess.Popen([sys.executable, "-c", "import time; time.sleep(120)"])
            record("prompt", messages=messages, child=child.pid if child else None)
            update("agent_thought_chunk", content={"type": "text", "text": "private-thinking " + "x" * 70000})
            if child:
                # Keep servicing stdin to acknowledge session/cancel while blocked.
                if question == "ignore-cancel":
                    record("ignore_cancel")
                    time.sleep(120)
                continue
            if question == "rpc-error":
                send({"id": request["id"], "error": {"code": -32603, "message": "private-acp-secret"}})
                continue
            if question == "bad-json":
                print("private-acp-secret", flush=True)
                continue
            if question == "wrong-session":
                send({"method": "session/update", "params": {
                    "sessionId": "wrong-session", "update": {"sessionUpdate": "agent_message_chunk", "content": {"type": "text", "text": "private-acp-secret"}},
                }})
            if question == "malformed-update":
                send({"method": "session/update", "params": {
                    "sessionId": SESSION, "update": {"sessionUpdate": "agent_message_chunk", "content": {"type": "text", "text": 123}},
                }})
            if question == "unsupported-content":
                update("agent_message_chunk", content={"type": "image", "data": "private-acp-secret", "mimeType": "image/png"})
            if question == "overflow-frame":
                answer("x" * (128 * 1024 + 1))
            if question == "overflow-wire":
                for _ in range(20):
                    update("agent_thought_chunk", content={"type": "text", "text": "x" * 70000})
            if question == "overflow-stderr":
                sys.stderr.write("private-acp-secret" * 3000)
                sys.stderr.flush()
            if question == "tool-call":
                update("tool_call", toolCallId="tool", title="private-acp-secret", kind="execute", status="completed")
            if question in ["permission", "read-file", "write-file", "terminal", "extension"]:
                methods = {"permission": "session/request_permission", "read-file": "fs/read_text_file",
                           "write-file": "fs/write_text_file", "terminal": "terminal/create", "extension": "_private/execute_sql"}
                send({"id": "server-request", "method": methods[question], "params": {
                    "sessionId": SESSION, "path": "/private/secret", "command": "private-acp-secret",
                    "toolCall": {"toolCallId": "tool", "title": "private-acp-secret"},
                    "options": [{"optionId": "allow", "name": "Approve", "kind": "allow_always"}],
                }})
                receive_reply("server-request")
            if question == "no-answer":
                result(prompt_id)
                continue
            if question == "long-answer":
                for _ in range(20):
                    answer("中" * 1000)
            else:
                answer("\x1b[31mMock ACP ")
                answer("answer grounded in query evidence.")
            result(prompt_id, "cancelled" if question == "failed-turn" else "max_tokens" if question == "max-tokens" else "end_turn")
        elif method == "session/cancel":
            record("cancel", params=params)
            if prompt_id is not None:
                result(prompt_id, "cancelled")
        elif "method" in request:
            record("other", request=request)
        else:
            record("client_reply", reply=request)


if __name__ == "__main__":
    main()
