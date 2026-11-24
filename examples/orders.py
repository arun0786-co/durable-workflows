"""An order: reserve stock, charge the card, wait up to a deadline for the
warehouse to confirm shipping; if it never does, refund and release. Kill
this script at any point and run it again: the orders carry on where they
were.

    python examples/orders.py            # uses orders.db in the current directory
    python -m durable show orders.db order-2
"""

import os
import sys
import threading
import time

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from durable import ActivityError, Client, RetryPolicy, Store, Worker, activity_context  # noqa: E402

DEADLINE = 3.0  # seconds here; days in real life


# ---- activities: the outside world. Each passes its idempotency key on, so
# a retry after a lost reply does not charge twice.

def reserve(order, items):
    print(f"  {order}: reserved {items} (key {activity_context().idempotency_key[-12:]})")
    return {"reservation": f"res-{order}"}


def charge(order, cents):
    if cents > 50_000:
        raise ValueError("card declined")
    print(f"  {order}: charged {cents / 100:.2f}")
    return {"charge": f"ch-{order}"}


def refund(order, charge_id):
    print(f"  {order}: refunded {charge_id}")


def release(order, reservation):
    print(f"  {order}: released {reservation}")


# ---- the workflow: plain async code, durable across crashes

async def order(ctx, order_id, items, cents):
    once = RetryPolicy(max_attempts=3, non_retryable=["ValueError"])
    res = await ctx.activity(reserve, order_id, items, retry=once)
    try:
        ch = await ctx.activity(charge, order_id, cents, retry=once)
    except ActivityError as e:
        await ctx.activity(release, order_id, res["reservation"])
        return f"declined: {e.message}"
    which, _ = await ctx.first(ctx.wait_signal("shipped"), ctx.sleep(DEADLINE))
    if which == 0:
        return "shipped"
    await ctx.activity(refund, order_id, ch["charge"])
    await ctx.activity(release, order_id, res["reservation"])
    return "not shipped in time: refunded"


def main():
    db = "orders.db"
    worker = Worker(Store(db), [order], [reserve, charge, refund, release], lease=5)
    stop = threading.Event()
    worker.run(stop, activity_threads=2)
    client = Client(Store(db))
    ids = [client.start(order, "order-1", ["book"], 1999, id="order-1"),
           client.start(order, "order-2", ["lamp", "bulb"], 4500, id="order-2"),
           client.start(order, "order-3", ["piano"], 990_000, id="order-3")]
    time.sleep(0.5)
    client.signal("order-1", "shipped")  # the warehouse confirms one of them
    for wid in ids:
        print(f"{wid}: {client.result(wid, timeout=30)}")
    stop.set()
    worker.shutdown()


if __name__ == "__main__":
    main()
