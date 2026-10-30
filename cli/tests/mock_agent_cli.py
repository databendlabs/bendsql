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

"""Mock Codex/Claude executable used only by agent_repl.py; no model calls."""

import json
import os
import subprocess
import sys
import time


def codex_app_server():
    record_path = os.environ.get("CLI_TEST_RECORD", os.path.join(os.environ["HOME"], "builtin_cli_requests.jsonl"))
    thread = "mock-thread"
    turn = "mock-turn"
    settings = {}
    question = None
    child = None
    def send(value):
        print(json.dumps(value), flush=True)
    def event(method, **params):
        send({"method": method, "params": params})
    for line in sys.stdin:
        frame = json.loads(line)
        method = frame.get("method")
        params = frame.get("params", {})
        if method == "initialize":
            send({"id": frame["id"], "result": {"userAgent": "mock"}})
        elif method == "thread/start":
            settings = params
            send({"id": frame["id"], "result": {"thread": {"id": thread, "ephemeral": True},
                  "approvalPolicy": "never", "sandbox": {"type": "readOnly"}}})
        elif method == "turn/start":
            messages = json.loads(params["input"][0]["text"].split("\n", 1)[1])
            question = json.loads(messages[-1]["content"])["question"]
            if question in ["block-cli", "stream-cancel", "stream-timeout"]:
                child = subprocess.Popen([sys.executable, "-c", "import time; time.sleep(120)"])
            try:
                terminal = os.open("/dev/tty", os.O_RDWR)
                os.close(terminal)
                controlling_terminal = True
            except OSError:
                controlling_terminal = False
            record = {"argv": sys.argv[1:], "env": dict(os.environ), "cwd": os.getcwd(),
                      "messages": messages, "pid": os.getpid(), "child": child.pid if child else None,
                      "policy": None, "directory_files": os.listdir(os.getcwd()),
                      "thread_settings": settings, "model": settings.get("model"),
                      "controlling_terminal": controlling_terminal}
            with open(record_path, "a") as file:
                file.write(json.dumps(record) + "\n")
            event("turn/started", threadId=thread, turn={"id": turn, "status": "inProgress"})
            send({"id": frame["id"], "result": {"turn": {"id": turn, "status": "inProgress"}}})
            if child:
                if question != "block-cli":
                    event("item/started", threadId=thread, turnId=turn,
                          item={"type": "agentMessage", "id": "message", "text": "", "phase": "final_answer"})
                    event("item/agentMessage/delta", threadId=thread, turnId=turn,
                          itemId="message", delta="Codex cancellable partial")
                continue
            if question.startswith("stream-"):
                # Commentary/reasoning and unannounced/unrelated message deltas
                # must never enter the live answer, even before its first frame.
                event("item/agentMessage/delta", threadId=thread, turnId=turn,
                      itemId="unannounced", delta="private-codex-unannounced")
                commentary = {"type": "agentMessage", "id": "commentary", "text": "", "phase": "commentary"}
                event("item/started", threadId=thread, turnId=turn, item=commentary)
                event("item/agentMessage/delta", threadId=thread, turnId=turn,
                      itemId="commentary", delta="private-codex-commentary")
                event("item/completed", threadId=thread, turnId=turn,
                      item=dict(commentary, text="private-codex-commentary"))
                event("item/reasoning/textDelta", threadId=thread, turnId=turn,
                      itemId="thinking", delta="private-codex-thinking")
                item = {"type": "agentMessage", "id": "message", "text": "",
                        "phase": None if question == "stream-legacy" else "final_answer"}
                if question != "stream-unannounced":
                    event("item/started", threadId=thread, turnId=turn, item=item)
                event("item/agentMessage/delta", threadId=thread, turnId=turn,
                      itemId="other-message", delta="private-codex-unrelated")
                first = "Codex provisional text" if question == "stream-authoritative" else "Codex first part."
                if question in ["stream-failed", "stream-disconnect", "stream-no-answer", "stream-tool-item",
                                "stream-tool-request", "stream-wrong-thread", "stream-wrong-turn"]:
                    first = "Codex discarded partial"
                if question != "stream-no-deltas":
                    event("item/agentMessage/delta", threadId=thread, turnId=turn,
                          itemId="message", delta=first)
                if question in ["stream-codex", "stream-authoritative"]:
                    # The PTY test releases this only after inspecting a live frame.
                    while not os.path.exists(record_path + ".release"):
                        time.sleep(0.02)
                else:
                    time.sleep(0.2)
                if question in ["stream-tool-item", "stream-tool-request"]:
                    if question == "stream-tool-item":
                        event("item/started", threadId=thread, turnId=turn,
                              item={"type": "newProviderToolCall", "id": "tool"})
                    else:
                        send({"id": "approval", "method": "item/commandExecution/requestApproval", "params": {}})
                    event("item/agentMessage/delta", threadId=thread, turnId=turn,
                          itemId="message", delta="private-codex-after-denial")
                    time.sleep(0.2)
                if question in ["stream-wrong-thread", "stream-wrong-turn"]:
                    event("item/agentMessage/delta", threadId="wrong" if question == "stream-wrong-thread" else thread,
                          turnId="wrong" if question == "stream-wrong-turn" else turn,
                          itemId="message", delta="private-codex-wrong-scope")
                    continue
                if question == "stream-failed":
                    event("turn/completed", threadId=thread,
                          turn={"id": turn, "status": "failed", "error": {"message": "private-cli-secret"}, "items": []})
                    continue
                if question == "stream-disconnect":
                    sys.exit(0)
                if question == "stream-no-answer":
                    event("turn/completed", threadId=thread,
                          turn={"id": turn, "status": "completed", "error": None, "items": []})
                    continue
                if question not in ["stream-no-deltas", "stream-authoritative"]:
                    event("item/agentMessage/delta", threadId=thread, turnId=turn,
                          itemId="message", delta=" Codex second part.")
                    time.sleep(0.1)
                item["text"] = "Codex authoritative answer." if question == "stream-authoritative" else first + " Codex second part."
                event("item/completed", threadId=thread, turnId=turn, item=item)
                event("turn/completed", threadId=thread,
                      turn={"id": turn, "status": "completed", "error": None, "items": [item]})
                continue
            if question == "exit-error":
                print("private-cli-secret", file=sys.stderr)
                sys.exit(2)
            if question == "bad-json":
                print("private-cli-secret", flush=True)
                continue
            if question == "overflow-stdout":
                sys.stdout.write("x" * (1024 * 1024 + 1) + "\n")
                sys.stdout.flush()
                continue
            if question == "overflow-stderr":
                sys.stderr.write("private-cli-secret" * 30000)
                sys.stderr.flush()
            if question == "future-tool-item":
                event("item/completed", threadId=thread, turnId=turn,
                      item={"type": "newProviderToolCall", "id": "future-tool"})
            if question == "future-tool-request":
                send({"id": "future-tool-request", "method": "item/futureExecution", "params": {}})
            if question == "future-metadata":
                event("item/completed", threadId=thread, turnId=turn,
                      item={"type": "newPassiveMetadata", "id": "meta"})
                send({"id": "server-meta", "method": "server/newMetadataRequest", "params": {}})
            if question == "failed-turn":
                event("turn/completed", threadId=thread, turn={"id": turn, "status": "failed", "error": {"message": "private-cli-secret"}, "items": []})
                continue
            answer = "\x1b[31mMock CLI answer grounded in query evidence."
            if question == "long-answer":
                answer = "中" * 8000
            item = {"type": "agentMessage", "id": "message", "text": answer, "phase": "final_answer"}
            event("item/started", threadId=thread, turnId=turn, item=dict(item, text=""))
            event("item/agentMessage/delta", threadId=thread, turnId=turn, itemId="message", delta=answer)
            event("item/completed", threadId=thread, turnId=turn, item=item)
            event("turn/completed", threadId=thread, turn={"id": turn, "status": "completed", "error": None, "items": [item]})
        elif method == "turn/interrupt":
            with open(record_path + ".interrupt", "a") as file:
                file.write(json.dumps({"pid": os.getpid(), "params": params}) + "\n")
            send({"id": frame["id"], "result": {}})
            event("turn/completed", threadId=thread, turn={"id": turn, "status": "interrupted", "error": None, "items": []})


def main():
    if "--version" in sys.argv:
        # BendSQL gates Codex on its version before sending context.
        print("codex-cli 0.144.6" if os.environ.get("CLI_TEST_OLD") else "codex-cli 0.154.0")
        return
    if "app-server" in sys.argv:
        codex_app_server()
        return
    pi = "--mode" in sys.argv
    record_path = os.environ.get("CLI_TEST_RECORD", os.path.join(os.environ["HOME"], "builtin_cli_requests.jsonl"))
    if "--help" in sys.argv:
        text = sys.stdin.read()
        flags = [arg for arg in sys.argv if arg.startswith("--")]
        if not pi:
            flags += ["--print", "--input-format", "--output-format", "--no-session-persistence",
                      "--tools", "--permission-mode", "--disable-slash-commands", "--safe-mode",
                      "--no-chrome", "--setting-sources", "--settings", "--strict-mcp-config", "--mcp-config"]
        if pi and "--model" not in flags:
            flags.append("--model")
        if os.environ.get("CLI_TEST_OLD"):
            flags.remove("--no-tools" if pi else "--safe-mode")
        with open(record_path + ".help", "a") as file:
            file.write(json.dumps({"stdin": text, "flags": flags}) + "\n")
        print(" ".join(flags))
        return
    # Produce more than a pipe buffer before reading input. The client must drain
    # stdout/stderr concurrently rather than write stdin and then wait.
    sys.stderr.write("diagnostic " * 1500)
    sys.stderr.flush()
    if pi:
        print(json.dumps({"type": "session", "version": 3, "id": "mock"}), flush=True)
        print(json.dumps({"type": "agent_start"}), flush=True)
        print(json.dumps({"type": "message_update", "assistantMessageEvent": {
            "type": "thinking_delta", "delta": "private-thinking " + "x" * 70000,
        }}), flush=True)
    text = sys.stdin.read()
    messages = json.loads(text.split("\n", 1)[1])
    question = json.loads(messages[-1]["content"])["question"]
    child = None
    if question == "block-cli":
        child = subprocess.Popen([sys.executable, "-c", "import time; time.sleep(120)"])
    record = {
        "argv": sys.argv[1:], "env": dict(os.environ), "cwd": os.getcwd(),
        "messages": messages, "pid": os.getpid(), "child": child.pid if child else None,
        "directory_files": os.listdir(os.getcwd()),
    }
    with open(record_path, "a") as file:
        file.write(json.dumps(record) + "\n")
    if child:
        child.wait()
        time.sleep(120)
    if question == "exit-error":
        print("private-cli-secret", file=sys.stderr)
        sys.exit(2)
    if question == "bad-json":
        print("private-cli-secret")
        return
    if question == "overflow-stdout":
        sys.stdout.write("x" * (1024 * 1024 + 1))
        sys.stdout.flush()
        return
    if question == "overflow-stderr":
        sys.stderr.write("private-cli-secret" * 30000)
        sys.stderr.flush()
    if question == "failed-turn":
        if pi:
            print(json.dumps({"type": "message_end", "message": {
                "role": "assistant", "stopReason": "error", "errorMessage": "private-cli-secret",
                "content": [],
            }}))
            print(json.dumps({"type": "agent_end", "willRetry": False}))
            print(json.dumps({"type": "agent_settled", "aborted": False}))
        else:
            print(json.dumps({"type": "result", "subtype": "error_during_execution", "is_error": True,
                              "result": "private-cli-secret"}))
        return
    if question == "tool-policy":
        if pi:
            print(json.dumps({"type": "tool_execution_start", "args": {"command": "private-cli-secret"}}))
    answer = "\x1b[31mMock CLI answer grounded in query evidence."
    if question == "long-answer":
        answer = "中" * 8000
    if pi:
        print(json.dumps({"type": "message_end", "message": {"role": "assistant", "stopReason": "stop",
                        "content": [{"type": "thinking", "thinking": "private-thinking"}, {"type": "text", "text": answer}]}}))
        print(json.dumps({"type": "agent_end", "willRetry": False}))
        print(json.dumps({"type": "agent_settled", "aborted": False}))
    else:
        print(json.dumps({"type": "result", "subtype": "success", "is_error": False,
                          "result": answer}))


if __name__ == "__main__":
    main()
