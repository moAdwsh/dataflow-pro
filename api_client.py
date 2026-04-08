"""
DataFlow Pro — API Client
==========================
Demonstrates every server endpoint with real requests.
Run this AFTER the server is up:

    # Terminal 1 — start the server
    uvicorn api_server:app --reload --port 8000

    # Terminal 2 — fire all requests
    python api_client.py

Requires: pip install httpx
"""

import httpx
import json

BASE_URL = "http://localhost:8000"
client   = httpx.Client(base_url=BASE_URL, timeout=30.0)


# ─────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────

def _print_section(title: str) -> None:
    print(f"\n{'═'*55}")
    print(f"  {title}")
    print(f"{'═'*55}")


def _req(method: str, path: str, **kwargs) -> dict:
    """Fires a request, prints it prettily, and returns the JSON body."""
    resp = getattr(client, method)(path, **kwargs)
    print(f"\n  {method.upper()} {path}")
    if kwargs.get("json"):
        print(f"  Payload : {json.dumps(kwargs['json'], ensure_ascii=False)}")
    print(f"  Status  : {resp.status_code}")
    try:
        data = resp.json()
        pretty = json.dumps(data, indent=4, ensure_ascii=False)
        # Trim very long previews so the terminal stays readable
        lines = pretty.splitlines()
        if len(lines) > 40:
            pretty = "\n".join(lines[:40]) + f"\n  … ({len(lines)-40} more lines)"
        print(f"  Response:\n{pretty}")
        return data
    except Exception:
        print(f"  Raw: {resp.text[:300]}")
        return {}


# ─────────────────────────────────────────────
# Health check
# ─────────────────────────────────────────────

def check_health() -> None:
    _print_section("Health Check")
    _req("get", "/")


# ─────────────────────────────────────────────
# Phase 1 — Sorting & Searching
# ─────────────────────────────────────────────

def demo_phase1() -> None:
    _print_section("Phase 1 — Query Optimizer (Sort & Search)")

    # Single algorithm
    _req("post", "/sort", json={"n": 300, "algorithm": "merge"})

    # Compare all algorithms
    _req("post", "/sort", json={"n": 200, "algorithm": "all"})

    # Binary search (default)
    _req("post", "/search", json={"n": 500, "target_index": 100, "search_type": "binary"})

    # Linear search
    _req("post", "/search", json={"n": 500, "target_index": 100, "search_type": "linear"})

    # Q3 sales slice
    _req("post", "/q3-sales", json={"n": 1000, "q3_start": 300000, "q3_end": 600000})


# ─────────────────────────────────────────────
# Phase 2 — Steps Tracker
# ─────────────────────────────────────────────

def demo_phase2() -> None:
    _print_section("Phase 2 — Applied Steps Tracker (Linked Lists)")

    # Reset first so demo is clean
    _req("delete", "/tracker/reset")

    # Add steps
    for step in ["Remove Nulls", "Change Type", "Filter Rows", "Rename Columns"]:
        _req("post", "/tracker/add", json={"step_name": step})

    # View full state
    _req("get", "/tracker/state")

    # Undo twice
    _req("post", "/tracker/undo")
    _req("post", "/tracker/undo")

    # Redo once
    _req("post", "/tracker/redo")

    # Final state
    _req("get", "/tracker/state")


# ─────────────────────────────────────────────
# Phase 3 — DAX Parser
# ─────────────────────────────────────────────

def demo_phase3() -> None:
    _print_section("Phase 3 — DAX Formula Parser (Stacks)")

    formulas = [
        "(15000 + 5000) * 2",
        "(100000 - 20000) / 4",
        "(3 + 5) * (2 ^ 3) - 4",
        "50000 * 0.14",           # VAT calculation
    ]
    for f in formulas:
        _req("post", "/dax/evaluate", json={"expression": f})

    # Direct postfix
    _req("post", "/dax/evaluate-postfix", json={"expression": "15000 5000 + 2 *"})

    # Edge case: division by zero
    print("\n  Testing division-by-zero guard:")
    _req("post", "/dax/evaluate", json={"expression": "100 / 0"})


# ─────────────────────────────────────────────
# Phase 4 — Live Buffer
# ─────────────────────────────────────────────

def demo_phase4() -> None:
    _print_section("Phase 4 — Live Data Buffer (Queues)")

    # Status before
    _req("get", "/buffer/status")

    # Simulate White Friday POS stream
    pos_rows = [
        {"txn": 1045, "branch": "Maadi",    "amt_egp": 850},
        {"txn": 1046, "branch": "Smouha",   "amt_egp": 3200},
        {"txn": 1047, "branch": "Zayed",    "amt_egp": 1750},
        {"txn": 1048, "branch": "Mansoura", "amt_egp": 620},
        {"txn": 1049, "branch": "Nasr City","amt_egp": 4100},
    ]
    for row in pos_rows:
        _req("post", "/buffer/enqueue", json=row)

    # Status after enqueue
    _req("get", "/buffer/status")

    # Process a batch of 3
    _req("post", "/buffer/process", json={"batch_size": 3})

    # Status after batch
    _req("get", "/buffer/status")

    # Flush the rest
    _req("delete", "/buffer/flush")


# ─────────────────────────────────────────────
# Phase 5 — Trees
# ─────────────────────────────────────────────

def demo_phase5() -> None:
    _print_section("Phase 5 — Hierarchical Matrix Builder (Trees)")

    # --- BST ---
    _req("delete", "/bst/reset")

    customers = [
        {"national_id": 29050112345678, "name": "Aya Hassan"},
        {"national_id": 30060223456789, "name": "Mahmoud Ali"},
        {"national_id": 28040334567890, "name": "Kareem Samy"},
        {"national_id": 31070445678901, "name": "Nour Tarek"},
        {"national_id": 27030556789012, "name": "Omar Walid"},
    ]
    for c in customers:
        _req("post", "/bst/insert", json=c)

    # Search
    _req("get", "/bst/search/30060223456789")

    # 404 case
    print("\n  Testing not-found guard:")
    _req("get", "/bst/search/99999999999999")

    # In-order traversal
    _req("get", "/bst/all")

    # --- Org Chart ---
    _req("get", "/org-chart")

    # Roll-up per manager
    for mgr in ["vp_cairo", "vp_alex", "ceo"]:
        _req("get", f"/org-chart/rollup/{mgr}")


# ─────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────

def main() -> None:
    print("\n╔══════════════════════════════════════════════════════╗")
    print("║   DataFlow Pro — API Client  (all endpoints)         ║")
    print("╚══════════════════════════════════════════════════════╝")
    print(f"  Target: {BASE_URL}")

    try:
        check_health()
    except httpx.ConnectError:
        print(
            "\n  [ERROR] Cannot reach the server.\n"
            "  Start it first:\n"
            "    cd src && uvicorn api_server:app --reload --port 8000\n"
        )
        return

    demo_phase1()
    demo_phase2()
    demo_phase3()
    demo_phase4()
    demo_phase5()

    print(f"\n{'═'*55}")
    print("  All requests completed.")
    print(f"{'═'*55}\n")


if __name__ == "__main__":
    main()
