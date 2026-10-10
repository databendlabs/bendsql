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


def main():
    codex = "--json" in sys.argv
    pi = "--mode" in sys.argv
    amp = "--stream-json" in sys.argv
    record_path = os.environ.get("CLI_TEST_RECORD", os.path.join(os.environ["HOME"], "builtin_cli_requests.jsonl"))
    policy = None
    if amp:
        with open(sys.argv[sys.argv.index("--settings-file") + 1]) as file:
            policy = json.load(file)
    if "--help" in sys.argv:
        text = sys.stdin.read()
        flags = [arg for arg in sys.argv if arg.startswith("--")]
        if pi and "--model" not in flags:
            flags.append("--model")
        if os.environ.get("CLI_TEST_OLD"):
            flags.remove("--settings-file" if amp else "--no-tools")
        with open(record_path + ".help", "a") as file:
            file.write(json.dumps({"stdin": text, "flags": flags, "policy": policy}) + "\n")
        print(" ".join(flags))
        return
    # Produce more than a pipe buffer before reading input. The client must drain
    # stdout/stderr concurrently rather than write stdin and then wait.
    sys.stderr.write("diagnostic " * 1500)
    sys.stderr.flush()
    if codex:
        print(json.dumps({"type": "item.completed", "item": {
            "type": "reasoning", "text": "x" * 70000,
        }}), flush=True)
    if pi:
        print(json.dumps({"type": "session", "version": 3, "id": "mock"}), flush=True)
        print(json.dumps({"type": "agent_start"}), flush=True)
        print(json.dumps({"type": "message_update", "assistantMessageEvent": {
            "type": "thinking_delta", "delta": "private-thinking " + "x" * 70000,
        }}), flush=True)
    if amp:
        print(json.dumps({"type": "system", "subtype": "init", "tools": [], "mcp_servers": []}), flush=True)
        print(json.dumps({"type": "assistant", "message": {"content": [
            {"type": "thinking", "thinking": "private-thinking " + "x" * 70000},
        ]}}), flush=True)
    text = sys.stdin.read()
    messages = json.loads(text.split("\n", 1)[1])
    question = json.loads(messages[-1]["content"])["question"]
    child = None
    if question == "block-cli":
        child = subprocess.Popen([sys.executable, "-c", "import time; time.sleep(120)"])
    record = {
        "argv": sys.argv[1:], "env": dict(os.environ), "cwd": os.getcwd(),
        "messages": messages, "pid": os.getpid(), "child": child.pid if child else None,
        "policy": policy, "directory_files": os.listdir(os.getcwd()),
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
        sys.stderr.write("private-cli-secret" * 3000)
        sys.stderr.flush()
        return
    if question == "failed-turn":
        if codex:
            print(json.dumps({"type": "turn.failed", "error": {"message": "private-cli-secret"}}))
        elif pi:
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
        if amp:
            print(json.dumps({"type": "assistant", "message": {"content": [{"type": "tool_use", "input": "private-cli-secret"}]}}))
    answer = "\x1b[31mMock CLI answer grounded in query evidence."
    if question == "long-answer":
        answer = "中" * 8000
    if codex:
        print(json.dumps({"type": "item.completed", "item": {
            "type": "agent_message", "text": answer,
        }}))
        print(json.dumps({"type": "turn.completed", "usage": {"input_tokens": 1}}))
    elif pi:
        print(json.dumps({"type": "message_end", "message": {"role": "assistant", "stopReason": "stop",
                        "content": [{"type": "thinking", "thinking": "private-thinking"}, {"type": "text", "text": answer}]}}))
        print(json.dumps({"type": "agent_end", "willRetry": False}))
        print(json.dumps({"type": "agent_settled", "aborted": False}))
    else:
        print(json.dumps({"type": "result", "subtype": "success", "is_error": False,
                          "result": answer}))


if __name__ == "__main__":
    main()
