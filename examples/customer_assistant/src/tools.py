import json
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
ORDERS = json.loads((ROOT / "data" / "orders.json").read_text())["orders"]


async def lookup_order(args: dict) -> dict:
    order_id = args["order_id"].strip().upper()
    return ORDERS.get(order_id, {"error": f"there is no order {order_id}"})
