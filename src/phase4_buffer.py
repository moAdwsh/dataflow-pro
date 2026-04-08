"""
DataFlow Pro - Phase 4: The Live Data Buffer
============================================
Business Goal: Handle live streaming sales data without crashing the server.

Implements three Queue representations to demonstrate performance trade-offs:
  1. ListQueue         → Python list   — O(n) dequeue (the "trap")
  2. LinkedListQueue   → Linked list   — O(1) enqueue & dequeue
  3. LiveIngestionQueue → collections.deque — O(1) both ends; production standard

White Friday scenario: transactions pour in faster than the DB can write,
so we buffer them in a FIFO queue and flush in configurable batch sizes.
"""

from collections import deque
import time


# ─────────────────────────────────────────────
# Queue v1 — Python List  (O(n) dequeue trap)
# ─────────────────────────────────────────────

class ListQueue:
    """
    Queue backed by a plain Python list.
    enqueue: O(1)  — append to end
    dequeue: O(n)  — list.pop(0) shifts every remaining element left
    ⚠  This becomes a bottleneck when thousands of rows hit simultaneously.
    """

    def __init__(self):
        self._buffer: list = []

    def enqueue(self, row: dict) -> None:
        self._buffer.append(row)

    def dequeue(self) -> dict | None:
        if self.is_empty():
            return None
        return self._buffer.pop(0)   # ← O(n) shift!

    def is_empty(self) -> bool:
        return len(self._buffer) == 0

    def size(self) -> int:
        return len(self._buffer)

    def __repr__(self) -> str:
        return f"ListQueue(size={self.size()})"


# ─────────────────────────────────────────────
# Queue v2 — Linked List  (O(1) both ends)
# ─────────────────────────────────────────────

class _QNode:
    def __init__(self, data: dict):
        self.data: dict = data
        self.next: "_QNode | None" = None


class LinkedListQueue:
    """
    Queue implemented with a singly linked list.
    Maintains both head (front) and tail (back) pointers so every
    enqueue and dequeue is O(1) — no shifting, no resizing.
    """

    def __init__(self):
        self._head: _QNode | None = None
        self._tail: _QNode | None = None
        self._size: int = 0

    def enqueue(self, row: dict) -> None:
        node = _QNode(row)
        if self._tail:
            self._tail.next = node
        self._tail = node
        if not self._head:
            self._head = node
        self._size += 1

    def dequeue(self) -> dict | None:
        if not self._head:
            return None
        value = self._head.data
        self._head = self._head.next
        if not self._head:
            self._tail = None
        self._size -= 1
        return value

    def is_empty(self) -> bool:
        return self._head is None

    def size(self) -> int:
        return self._size

    def __repr__(self) -> str:
        return f"LinkedListQueue(size={self.size()})"


# ─────────────────────────────────────────────
# Queue v3 — deque  (Production Standard)
# ─────────────────────────────────────────────

class LiveIngestionQueue:
    """
    Production-grade FIFO queue using collections.deque.
    Both appendright (enqueue) and popleft (dequeue) are O(1) — implemented
    in C under the hood, making it faster than a pure-Python linked list.

    White Friday use-case:
      • POS terminals enqueue rows as they arrive.
      • A background worker calls process_batch() to flush to Power BI datasets.
    """

    def __init__(self):
        self._buffer: deque = deque()

    def enqueue_row(self, row_data: dict) -> None:
        """Adds a new transaction row to the back of the queue — O(1)."""
        self._buffer.append(row_data)
        print(f"[Buffer]   Enqueued → {row_data}")

    def process_batch(self, batch_size: int) -> list[dict]:
        """
        Removes up to batch_size items from the front of the queue — O(k).
        Simulates a flush to the NileMart Power BI dataset endpoint.
        """
        processed: list[dict] = []
        for _ in range(batch_size):
            if not self._buffer:
                break
            processed.append(self._buffer.popleft())
        print(f"[Buffer]   Processed {len(processed)} transaction(s). "
              f"Pushing to NileMart Power BI Datasets… ({self.size()} remaining)")
        return processed

    def is_empty(self) -> bool:
        return len(self._buffer) == 0

    def size(self) -> int:
        return len(self._buffer)

    def __repr__(self) -> str:
        return f"LiveIngestionQueue(size={self.size()})"


# ─────────────────────────────────────────────
# Performance Benchmark
# ─────────────────────────────────────────────

def benchmark_queues(n: int = 10_000) -> None:
    """Demonstrates the O(n) vs O(1) dequeue cost for list vs deque."""
    rows = [{"txn": i, "branch": "Maadi", "amt_egp": i * 10} for i in range(n)]

    print(f"\n  Queue Dequeue Benchmark  ({n:,} records)")
    print(f"{'─'*45}")

    # List-backed queue
    lq = ListQueue()
    for r in rows:
        lq.enqueue(r)
    start = time.perf_counter()
    while not lq.is_empty():
        lq.dequeue()
    print(f"  {'ListQueue (O(n))':<28} {time.perf_counter() - start:.4f} sec")

    # Linked-list queue
    llq = LinkedListQueue()
    for r in rows:
        llq.enqueue(r)
    start = time.perf_counter()
    while not llq.is_empty():
        llq.dequeue()
    print(f"  {'LinkedListQueue (O(1))':<28} {time.perf_counter() - start:.4f} sec")

    # deque queue
    dq = deque(rows)
    start = time.perf_counter()
    while dq:
        dq.popleft()
    print(f"  {'deque (O(1), C-speed)':<28} {time.perf_counter() - start:.4f} sec")
    print(f"{'─'*45}")


# ─────────────────────────────────────────────
# Demo / Self-test
# ─────────────────────────────────────────────

def demo() -> None:
    print("\n========== Phase 4: Live Data Buffer ==========")

    buffer = LiveIngestionQueue()

    # Simulate White Friday POS stream
    buffer.enqueue_row({"txn": 1045, "branch": "Maadi",   "amt_egp": 850})
    buffer.enqueue_row({"txn": 1046, "branch": "Smouha",  "amt_egp": 3_200})
    buffer.enqueue_row({"txn": 1047, "branch": "Zayed",   "amt_egp": 1_750})
    buffer.enqueue_row({"txn": 1048, "branch": "Mansoura","amt_egp": 620})

    print(f"\n[Buffer]   Queue depth: {buffer.size()} rows")
    batch = buffer.process_batch(2)
    print(f"[Buffer]   Batch payload: {batch}")
    print(f"[Buffer]   Queue depth after flush: {buffer.size()} rows")

    # Performance comparison
    benchmark_queues(n=5_000)


if __name__ == "__main__":
    demo()
