"""Looking inside a database:

    python -m durable list engine.db
    python -m durable show engine.db WORKFLOW_ID
"""

import json
import sys

from .runtime import COMMANDS
from .store import Store


def show(store, wid):
    d = store.describe(wid)
    print(f"{wid}  run {d['run_id']}  {d['status']}  result {json.dumps(d['result'])}")
    if d["task_error"]:
        print(f"  workflow task failing: {d['task_error']}")
    for e in store.history(d["run_id"]):
        fields = {k: v for k, v in e.items() if k not in ("id", "type")}
        pad = "    " if e["type"] in COMMANDS else "  "
        print(f"{e['id']:>4}{pad}{e['type']:<18} {json.dumps(fields)[:100]}")


def main():
    if len(sys.argv) < 3 or sys.argv[1] not in ("list", "show"):
        print(__doc__)
        sys.exit(1)
    store = Store(sys.argv[2])
    if sys.argv[1] == "list":
        for r in store.runs():
            print(f"{r['workflow_id']:<24} {r['workflow']:<16} {r['status']:<10} events {r['last_event']}")
    else:
        show(store, sys.argv[3])


if __name__ == "__main__":
    main()
