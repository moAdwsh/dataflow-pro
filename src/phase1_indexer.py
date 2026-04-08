"""
DataFlow Pro - Phase 1: The Query Optimizer
============================================
Business Goal: Speed up data retrieval for NileMart's daily sales fact tables.

Implements:
  - Bubble Sort, Insertion Sort, Selection Sort  (naive O(n²) algorithms)
  - Merge Sort, Quick Sort                        (optimized O(n log n) algorithms)
  - Timsort Benchmark vs Python built-in
  - Linear Search & Binary Search
  - bisect for O(log n) time-range slicing (Q3 Sales)
"""

import random
import time
import bisect


# ─────────────────────────────────────────────
# Data Generator
# ─────────────────────────────────────────────

def generate_transactions(n: int = 10_000) -> list[dict]:
    """Generates n mock NileMart transaction records with random IDs and amounts."""
    return [
        {"id": random.randint(1_000, 999_999), "amount": random.randint(100, 10_000)}
        for _ in range(n)
    ]


# ─────────────────────────────────────────────
# Sorting Algorithms
# ─────────────────────────────────────────────

def bubble_sort(data: list[dict]) -> list[dict]:
    """
    Bubble Sort — O(n²) time, O(1) space.
    Repeatedly swaps adjacent elements if they're in the wrong order.
    Terrible at scale; included to demonstrate why optimized sorts matter.
    """
    arr = data.copy()
    n = len(arr)
    for i in range(n):
        for j in range(n - i - 1):
            if arr[j]["id"] > arr[j + 1]["id"]:
                arr[j], arr[j + 1] = arr[j + 1], arr[j]
    return arr


def insertion_sort(data: list[dict]) -> list[dict]:
    """
    Insertion Sort — O(n²) time, O(1) space.
    Builds a sorted portion one element at a time.
    Efficient for nearly-sorted data; still fails at 10k+ rows.
    """
    arr = data.copy()
    for i in range(1, len(arr)):
        key = arr[i]
        j = i - 1
        while j >= 0 and arr[j]["id"] > key["id"]:
            arr[j + 1] = arr[j]
            j -= 1
        arr[j + 1] = key
    return arr


def selection_sort(data: list[dict]) -> list[dict]:
    """
    Selection Sort — O(n²) time, O(1) space.
    Finds the minimum element and places it at the start each pass.
    Performs the fewest swaps of O(n²) sorts, but still too slow for production.
    """
    arr = data.copy()
    n = len(arr)
    for i in range(n):
        min_idx = i
        for j in range(i + 1, n):
            if arr[j]["id"] < arr[min_idx]["id"]:
                min_idx = j
        arr[i], arr[min_idx] = arr[min_idx], arr[i]
    return arr


def merge_sort(data: list[dict]) -> list[dict]:
    """
    Merge Sort — O(n log n) time, O(n) space.
    Divide-and-conquer: recursively splits, then merges sorted halves.
    Stable and predictable — ideal for our ETL pipeline.
    """
    if len(data) <= 1:
        return data
    mid = len(data) // 2
    left = merge_sort(data[:mid])
    right = merge_sort(data[mid:])
    return _merge(left, right)


def _merge(left: list[dict], right: list[dict]) -> list[dict]:
    result, i, j = [], 0, 0
    while i < len(left) and j < len(right):
        if left[i]["id"] <= right[j]["id"]:
            result.append(left[i])
            i += 1
        else:
            result.append(right[j])
            j += 1
    result.extend(left[i:])
    result.extend(right[j:])
    return result


def quick_sort(data: list[dict]) -> list[dict]:
    """
    Quick Sort — O(n log n) average, O(n²) worst-case, O(log n) space.
    Partitions around a pivot. Fastest in practice on random data.
    Our ETL pipeline uses this as the primary sort for transaction IDs.
    """
    if len(data) <= 1:
        return data
    pivot = data[0]
    less    = [x for x in data[1:] if x["id"] <= pivot["id"]]
    greater = [x for x in data[1:] if x["id"] >  pivot["id"]]
    return quick_sort(less) + [pivot] + quick_sort(greater)


# ─────────────────────────────────────────────
# Timsort Benchmark
# ─────────────────────────────────────────────

def run_benchmark(n: int = 5_000) -> None:
    """
    Benchmarks all sorting algorithms against Python's built-in Timsort.
    Timsort is a hybrid merge/insertion sort optimised for real-world data.
    """
    data = generate_transactions(n)
    print(f"\n{'─'*45}")
    print(f"  Sorting Benchmark  ({n:,} records)")
    print(f"{'─'*45}")

    algorithms = [bubble_sort, insertion_sort, selection_sort, merge_sort, quick_sort]
    for func in algorithms:
        start = time.perf_counter()
        func(data)
        elapsed = time.perf_counter() - start
        print(f"  {func.__name__:<20} {elapsed:.4f} sec")

    # Python built-in Timsort
    start = time.perf_counter()
    sorted(data, key=lambda x: x["id"])
    elapsed = time.perf_counter() - start
    print(f"  {'python sort()  (Timsort)':<20} {elapsed:.4f} sec")
    print(f"{'─'*45}")


# ─────────────────────────────────────────────
# Search Algorithms
# ─────────────────────────────────────────────

def linear_search(data: list[dict], target_id: int) -> dict | None:
    """
    Linear Search — O(n) time.
    Scans every element. Works on unsorted data, but unusable at scale.
    """
    for item in data:
        if item["id"] == target_id:
            return item
    return None


def binary_search(sorted_data: list[dict], target_id: int) -> dict | None:
    """
    Binary Search — O(log n) time.
    Requires sorted input. Halves the search space each step.
    Essential for the fraud-detection lookup engine.
    """
    left, right = 0, len(sorted_data) - 1
    while left <= right:
        mid = (left + right) // 2
        mid_id = sorted_data[mid]["id"]
        if mid_id == target_id:
            return sorted_data[mid]
        elif mid_id < target_id:
            left = mid + 1
        else:
            right = mid - 1
    return None


# ─────────────────────────────────────────────
# Q3 Sales Slicer (bisect)
# ─────────────────────────────────────────────

def get_q3_sales(sorted_data: list[dict], q3_start: int = 300_000, q3_end: int = 600_000) -> list[dict]:
    """
    Uses Python's bisect module for O(log n) time-range slicing.
    Mimics a SQL WHERE id BETWEEN q3_start AND q3_end on a sorted index.
    """
    ids = [item["id"] for item in sorted_data]
    left_idx  = bisect.bisect_left(ids, q3_start)
    right_idx = bisect.bisect_right(ids, q3_end)
    return sorted_data[left_idx:right_idx]


# ─────────────────────────────────────────────
# Demo / Self-test
# ─────────────────────────────────────────────

def demo() -> None:
    print("\n========== Phase 1: Query Optimizer ==========")
    data = generate_transactions(1_000)

    # Sort
    sorted_data = merge_sort(data)
    print(f"[Indexer]  Sorted {len(sorted_data):,} records via Merge Sort.")

    # Search
    target_id = sorted_data[100]["id"]
    linear_result = linear_search(data, target_id)
    binary_result = binary_search(sorted_data, target_id)
    print(f"[Search]   Linear  → {linear_result}")
    print(f"[Search]   Binary  → {binary_result}")

    # Q3 Sales slice
    q3 = get_q3_sales(sorted_data)
    print(f"[bisect]   Q3 Sales slice → {len(q3)} records (IDs 300k–600k)")

    # Benchmark
    run_benchmark(n=2_000)


if __name__ == "__main__":
    demo()
