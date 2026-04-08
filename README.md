# DataFlow Pro — NileMart ETL & Analytics Engine

> ITI PortSaid | PowerBI46R2 | DSA Project

A high-performance Python ETL engine that cleans, sorts, and structures data
before it reaches Power BI — built on Data Structures & Algorithms.

---

## Project Structure

```
dataflow_pro/
├── src/
│   ├── phase1_indexer.py   # Sorting & Searching
│   ├── phase2_tracker.py   # Linked Lists (Singly + Doubly)
│   ├── phase3_parser.py    # Stacks + DAX Expression Evaluator
│   ├── phase4_buffer.py    # Queues (List / Linked List / deque)
│   ├── phase5_trees.py     # BST + N-ary Org Tree
│   └── main.py             # CLI entry point
├── data/                   # Place mock CSV/JSON files here
├── requirements.txt
└── README.md
```

---

## Setup

```bash
pip install -r requirements.txt
cd src
```

---

## Running

```bash
# Interactive menu
python main.py

# Run all five phase demos at once
python main.py --demo

# Sorting & queue benchmarks only
python main.py --benchmark

# Or run any phase in isolation
python phase1_indexer.py
python phase2_tracker.py
# … etc.
```

---

## Phase Summary

| Phase | Data Structure | Business Use-Case |
|-------|---------------|-------------------|
| 1 | Sorting + Binary Search | Index 10k transaction records; fraud ID lookup |
| 2 | Singly + Doubly Linked List | Power Query Applied Steps + Undo/Redo |
| 3 | Stack (Array & Linked List) | DAX KPI formula evaluator |
| 4 | Queue (List → Linked List → deque) | White Friday live POS stream buffer |
| 5 | BST + N-ary Tree (anytree) | Customer dimension index + org roll-up |

---

## Big-O Performance Notes

### Sorting
| Algorithm | Time | Space | Verdict |
|-----------|------|-------|---------|
| Bubble Sort | O(n²) | O(1) | ❌ Fails at 10k+ rows |
| Insertion Sort | O(n²) | O(1) | ❌ Only good for near-sorted data |
| Selection Sort | O(n²) | O(1) | ❌ Fewest swaps but still O(n²) |
| Merge Sort | O(n log n) | O(n) | ✅ Stable, predictable, ETL-safe |
| Quick Sort | O(n log n) avg | O(log n) | ✅ Fastest in practice on random data |
| Python sort() (Timsort) | O(n log n) | O(n) | ✅✅ Best overall — use in production |

### Queue Dequeue Cost
| Implementation | Enqueue | Dequeue | Verdict |
|----------------|---------|---------|---------|
| Python list | O(1) | O(n) ← shift | ❌ Kills throughput under load |
| Linked List | O(1) | O(1) | ✅ Correct but pure-Python overhead |
| collections.deque | O(1) | O(1) | ✅✅ C-speed, use in production |

### BST vs Linear Scan
| Operation | Linear Scan | BST (balanced) |
|-----------|-------------|---------------|
| Search | O(n) | O(log n) |
| Insert | O(1) append | O(log n) |

### Linked List Undo/Redo
Doubly Linked List gives O(1) undo and redo — no dataset reload, no index rebuild.
