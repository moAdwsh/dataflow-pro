"""
DataFlow Pro — Main Application
================================
NileMart ETL & Analytics Processing Engine

Orchestrates all five phases as a unified CLI tool.

Usage:
    python main.py              # interactive menu
    python main.py --demo       # run all phase demos non-interactively
    python main.py --benchmark  # run sorting & queue benchmarks only
"""

import sys
import os
import argparse

# ── Make sure src/ is on the path when running from project root ──────────────
_SRC = os.path.dirname(os.path.abspath(__file__))
if _SRC not in sys.path:
    sys.path.insert(0, _SRC)

# ── Phase imports ─────────────────────────────────────────────────────────────
from phase1_indexer import (
    generate_transactions, merge_sort, quick_sort,
    linear_search, binary_search, get_q3_sales, run_benchmark,
)
from phase2_tracker import AppliedStepsTracker, AdvancedTracker
from phase3_parser  import DAXEvaluator
from phase4_buffer  import LiveIngestionQueue, benchmark_queues
from phase5_trees   import DimensionIndex, OrgChartAnalyzer


# ─────────────────────────────────────────────
# Header
# ─────────────────────────────────────────────

HEADER = """
╔══════════════════════════════════════════════════════╗
║      DataFlow Pro — NileMart ETL & Analytics         ║
║      Powered by DSA  |  ITI PortSaid PowerBI46R2     ║
╚══════════════════════════════════════════════════════╝"""

MENU = """
  Main Menu:
  ─────────────────────────────────────────────
  1. Phase 1 — Query Optimizer (Sort & Search)
  2. Phase 2 — Applied Steps Tracker (Linked Lists)
  3. Phase 3 — DAX Formula Parser  (Stacks)
  4. Phase 4 — Live Data Buffer    (Queues)
  5. Phase 5 — Org Chart & BST     (Trees)
  6. Run ALL phase demos
  7. Run Benchmarks (Sorting + Queue)
  0. Exit
  ─────────────────────────────────────────────"""


# ─────────────────────────────────────────────
# Phase 1 interactive flow
# ─────────────────────────────────────────────

def run_phase1() -> None:
    print("\n──── Phase 1: Query Optimizer ────")
    n = _ask_int("Number of records to generate (default 1000): ", default=1_000)
    data = generate_transactions(n)
    print(f"[Phase 1]  Generated {n:,} transactions.")

    algo = _ask_choice(
        "Choose sort algorithm [merge / quick / both] (default: both): ",
        options={"merge", "quick", "both", ""},
        default="both",
    )

    if algo in ("merge", "both"):
        sorted_m = merge_sort(data)
        print(f"[Phase 1]  Merge Sort complete. First ID: {sorted_m[0]['id']}")

    if algo in ("quick", "both"):
        sorted_q = quick_sort(data)
        print(f"[Phase 1]  Quick Sort complete. First ID: {sorted_q[0]['id']}")

    sorted_data = merge_sort(data)
    target = sorted_data[len(sorted_data) // 2]["id"]

    lin  = linear_search(data, target)
    bsrch = binary_search(sorted_data, target)
    print(f"[Phase 1]  Linear Search → {lin}")
    print(f"[Phase 1]  Binary Search → {bsrch}")

    q3 = get_q3_sales(sorted_data)
    print(f"[Phase 1]  Q3 slice (IDs 300k–600k): {len(q3)} records")


# ─────────────────────────────────────────────
# Phase 2 interactive flow
# ─────────────────────────────────────────────

def run_phase2() -> None:
    print("\n──── Phase 2: Applied Steps Tracker ────")
    print("Using AdvancedTracker (Doubly Linked List with undo/redo)")

    tracker = AdvancedTracker()
    while True:
        print("\n  [a] Add step  [u] Undo  [r] Redo  [s] Show all  [q] Back")
        cmd = input("  > ").strip().lower()
        if cmd == "a":
            step = input("  Step name: ").strip()
            if step:
                tracker.add_step(step)
        elif cmd == "u":
            tracker.undo()
        elif cmd == "r":
            tracker.redo()
        elif cmd == "s":
            tracker.show_all()
            tracker.show_current()
        elif cmd == "q":
            break
        else:
            print("  Unknown command.")


# ─────────────────────────────────────────────
# Phase 3 interactive flow
# ─────────────────────────────────────────────

def run_phase3() -> None:
    print("\n──── Phase 3: DAX Formula Parser ────")
    print("Enter an infix expression (e.g.  (15000 + 5000) * 2 )")
    print("Or a postfix expression   (e.g.  15000 5000 + 2 * )  prefixed with 'p:'")
    print("Type 'q' to go back.\n")

    evaluator = DAXEvaluator()
    while True:
        expr = input("  DAX > ").strip()
        if not expr or expr.lower() == "q":
            break
        try:
            if expr.lower().startswith("p:"):
                result = evaluator.evaluate_postfix(expr[2:].strip())
            else:
                result = evaluator.evaluate(expr)
            print(f"  Result: {result:,.4f}\n")
        except Exception as exc:
            print(f"  [Error] {exc}\n")


# ─────────────────────────────────────────────
# Phase 4 interactive flow
# ─────────────────────────────────────────────

def run_phase4() -> None:
    print("\n──── Phase 4: Live Data Buffer ────")
    buffer = LiveIngestionQueue()

    # Pre-load some White Friday rows
    seed_rows = [
        {"txn": 1045, "branch": "Maadi",    "amt_egp": 850},
        {"txn": 1046, "branch": "Smouha",   "amt_egp": 3_200},
        {"txn": 1047, "branch": "Zayed",    "amt_egp": 1_750},
        {"txn": 1048, "branch": "Mansoura", "amt_egp": 620},
    ]
    print("Pre-loading 4 POS rows into buffer…")
    for row in seed_rows:
        buffer.enqueue_row(row)

    while True:
        print(f"\n  Buffer depth: {buffer.size()} row(s)")
        print("  [e] Enqueue row  [p] Process batch  [q] Back")
        cmd = input("  > ").strip().lower()
        if cmd == "e":
            try:
                txn    = int(input("    Transaction ID : "))
                branch = input("    Branch name    : ").strip()
                amt    = int(input("    Amount (EGP)   : "))
                buffer.enqueue_row({"txn": txn, "branch": branch, "amt_egp": amt})
            except ValueError:
                print("  [Error] Invalid input.")
        elif cmd == "p":
            size = _ask_int("    Batch size (default 2): ", default=2)
            batch = buffer.process_batch(size)
            print(f"    Payload: {batch}")
        elif cmd == "q":
            break
        else:
            print("  Unknown command.")


# ─────────────────────────────────────────────
# Phase 5 interactive flow
# ─────────────────────────────────────────────

def run_phase5() -> None:
    print("\n──── Phase 5: Hierarchical Matrix Builder ────")

    # BST
    dim = DimensionIndex()
    seed_customers = [
        (29050112345678, "Aya Hassan"),
        (30060223456789, "Mahmoud Ali"),
        (28040334567890, "Kareem Samy"),
        (31070445678901, "Nour Tarek"),
        (27030556789012, "Omar Walid"),
    ]
    for nid, name in seed_customers:
        dim.insert(nid, name)
    print(f"[BST]  Dimension index loaded with {len(dim)} customers.")

    while True:
        print("\n  [s] Search customer  [i] In-order list  [o] Org Chart  [r] Roll-up  [q] Back")
        cmd = input("  > ").strip().lower()
        if cmd == "s":
            try:
                nid = int(input("    Enter National ID: "))
                result = dim.search(nid)
                print(f"    → {result if result else 'Not found'}")
            except ValueError:
                print("  [Error] National ID must be a number.")
        elif cmd == "i":
            print("\n  [BST] All customers (sorted):")
            for nid, name in dim.in_order():
                print(f"    {nid}  →  {name}")
        elif cmd == "o":
            org = OrgChartAnalyzer()
            org.display_chart()
        elif cmd == "r":
            org = OrgChartAnalyzer()
            org.display_chart()
            org.print_roll_up_report()
        elif cmd == "q":
            break
        else:
            print("  Unknown command.")


# ─────────────────────────────────────────────
# Run all demos (non-interactive)
# ─────────────────────────────────────────────

def run_all_demos() -> None:
    from phase1_indexer import demo as d1
    from phase2_tracker import demo as d2
    from phase3_parser  import demo as d3
    from phase4_buffer  import demo as d4
    from phase5_trees   import demo as d5

    for demo_fn in [d1, d2, d3, d4, d5]:
        demo_fn()


# ─────────────────────────────────────────────
# Helper utilities
# ─────────────────────────────────────────────

def _ask_int(prompt: str, default: int) -> int:
    raw = input(f"  {prompt}").strip()
    try:
        return int(raw) if raw else default
    except ValueError:
        return default


def _ask_choice(prompt: str, options: set, default: str) -> str:
    raw = input(f"  {prompt}").strip().lower()
    return raw if raw in options else default


# ─────────────────────────────────────────────
# CLI entry point
# ─────────────────────────────────────────────

def parse_args():
    parser = argparse.ArgumentParser(description="DataFlow Pro — NileMart ETL Engine")
    parser.add_argument("--demo",      action="store_true", help="Run all phase demos")
    parser.add_argument("--benchmark", action="store_true", help="Run benchmarks only")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    print(HEADER)

    if args.demo:
        run_all_demos()
        return

    if args.benchmark:
        print("\n========== Benchmarks ==========")
        run_benchmark(n=5_000)
        benchmark_queues(n=10_000)
        return

    # ── Interactive menu ─────────────────────
    ACTIONS = {
        "1": run_phase1,
        "2": run_phase2,
        "3": run_phase3,
        "4": run_phase4,
        "5": run_phase5,
        "6": run_all_demos,
        "7": lambda: (run_benchmark(5_000), benchmark_queues(10_000)),
    }

    while True:
        print(MENU)
        choice = input("  Select: ").strip()

        if choice == "0":
            print("\n  Shutting down DataFlow Pro. Masalama! 🇪🇬\n")
            sys.exit(0)

        action = ACTIONS.get(choice)
        if action:
            action()
        else:
            print("  [!] Invalid choice — please enter 0–7.")


if __name__ == "__main__":
    main()
