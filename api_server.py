"""
DataFlow Pro — FastAPI Server
==============================
Exposes all five DSA phases as REST API endpoints.

Run locally:
    uvicorn api_server:app --reload --port 8000

Docs:
    http://localhost:8000/docs      ← Swagger UI
    http://localhost:8000/redoc     ← ReDoc
"""

import sys
import os

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field
from typing import Any

# ── Phase imports ─────────────────────────────────────────────────────────────
from phase1_indexer import (
    generate_transactions, bubble_sort, insertion_sort, selection_sort,
    merge_sort, quick_sort, linear_search, binary_search, get_q3_sales,
)
from phase2_tracker import AdvancedTracker
from phase3_parser  import DAXEvaluator
from phase4_buffer  import LiveIngestionQueue
from phase5_trees   import DimensionIndex, OrgChartAnalyzer


# ─────────────────────────────────────────────
# App bootstrap
# ─────────────────────────────────────────────

app = FastAPI(
    title="DataFlow Pro API",
    description=(
        "NileMart ETL & Analytics Engine — exposes Sorting, Linked Lists, "
        "Stacks, Queues, and Trees as REST endpoints."
    ),
    version="1.0.0",
    contact={"name": "ITI PortSaid | PowerBI46R2"},
)

# ── Shared in-memory state (single-process demo) ──────────────────────────────
_tracker = AdvancedTracker()
_evaluator = DAXEvaluator()
_buffer = LiveIngestionQueue()
_dim_index = DimensionIndex()
_org_chart = OrgChartAnalyzer()


# ─────────────────────────────────────────────
# Request / Response schemas
# ─────────────────────────────────────────────

class SortRequest(BaseModel):
    n: int = Field(default=500, ge=1, le=10_000, description="Number of random transactions to generate")
    algorithm: str = Field(
        default="merge",
        description="One of: bubble, insertion, selection, merge, quick, all",
    )

class SearchRequest(BaseModel):
    n: int = Field(default=500, ge=1, le=5_000, description="Dataset size")
    target_index: int = Field(default=50, ge=0, description="Index in sorted array to look up")
    search_type: str = Field(default="binary", description="'linear' or 'binary'")

class Q3Request(BaseModel):
    n: int = Field(default=1_000, ge=10, le=10_000)
    q3_start: int = Field(default=300_000)
    q3_end:   int = Field(default=600_000)

class StepRequest(BaseModel):
    step_name: str = Field(..., min_length=1, description="ETL transformation step label")

class DAXRequest(BaseModel):
    expression: str = Field(..., description="Infix expression, e.g. '(15000 + 5000) * 2'")

class PostfixRequest(BaseModel):
    expression: str = Field(..., description="Postfix (RPN) expression, e.g. '15000 5000 + 2 *'")

class TransactionRow(BaseModel):
    txn:    int   = Field(..., description="Transaction ID")
    branch: str   = Field(..., description="Branch name")
    amt_egp: int  = Field(..., description="Amount in EGP")

class BatchRequest(BaseModel):
    batch_size: int = Field(default=2, ge=1, le=100)

class CustomerRequest(BaseModel):
    national_id: int = Field(..., description="14-digit Egyptian National ID")
    name:        str = Field(..., min_length=1)

class SearchCustomerRequest(BaseModel):
    national_id: int


# ─────────────────────────────────────────────
# Root
# ─────────────────────────────────────────────

@app.get("/", tags=["Health"])
def root():
    return {
        "service": "DataFlow Pro",
        "status":  "running",
        "phases":  [
            "Phase 1 — /sort, /search, /q3-sales",
            "Phase 2 — /tracker/*",
            "Phase 3 — /dax/*",
            "Phase 4 — /buffer/*",
            "Phase 5 — /bst/*, /org-chart/*",
        ],
        "docs": "/docs",
    }


# ─────────────────────────────────────────────
# Phase 1 — Sorting & Searching
# ─────────────────────────────────────────────

SORT_MAP = {
    "bubble":    bubble_sort,
    "insertion": insertion_sort,
    "selection": selection_sort,
    "merge":     merge_sort,
    "quick":     quick_sort,
}

@app.post("/sort", tags=["Phase 1 — Query Optimizer"])
def sort_transactions(req: SortRequest) -> dict:
    """
    Generates n random NileMart transaction records and sorts them.
    Returns the first 20 sorted records + timing info.
    """
    import time

    algo = req.algorithm.lower()
    if algo not in {*SORT_MAP, "all"}:
        raise HTTPException(status_code=400, detail=f"Unknown algorithm '{algo}'. Choose from: bubble, insertion, selection, merge, quick, all")

    data = generate_transactions(req.n)

    if algo == "all":
        results = {}
        for name, fn in SORT_MAP.items():
            t0 = time.perf_counter()
            sorted_data = fn(data)
            elapsed = round(time.perf_counter() - t0, 4)
            results[name] = {"time_sec": elapsed, "first_5": sorted_data[:5]}
        return {"n": req.n, "results": results}

    fn = SORT_MAP[algo]
    t0 = time.perf_counter()
    sorted_data = fn(data)
    elapsed = round(time.perf_counter() - t0, 4)

    return {
        "algorithm":   algo,
        "n":           req.n,
        "time_sec":    elapsed,
        "preview":     sorted_data[:20],
        "total_sorted": len(sorted_data),
    }


@app.post("/search", tags=["Phase 1 — Query Optimizer"])
def search_transaction(req: SearchRequest) -> dict:
    """
    Generates a dataset, sorts it with merge sort, then searches for
    the record at target_index using linear or binary search.
    """
    import time

    data = generate_transactions(req.n)
    sorted_data = merge_sort(data)

    idx = min(req.target_index, len(sorted_data) - 1)
    target_id = sorted_data[idx]["id"]

    if req.search_type == "linear":
        t0 = time.perf_counter()
        result = linear_search(data, target_id)
        elapsed = round(time.perf_counter() - t0, 6)
    elif req.search_type == "binary":
        t0 = time.perf_counter()
        result = binary_search(sorted_data, target_id)
        elapsed = round(time.perf_counter() - t0, 6)
    else:
        raise HTTPException(status_code=400, detail="search_type must be 'linear' or 'binary'")

    return {
        "search_type": req.search_type,
        "n":           req.n,
        "target_id":   target_id,
        "result":      result,
        "time_sec":    elapsed,
    }


@app.post("/q3-sales", tags=["Phase 1 — Query Optimizer"])
def q3_sales(req: Q3Request) -> dict:
    """
    Uses Python's bisect module to slice 'Q3 Sales' from a sorted dataset in O(log n).
    """
    data = generate_transactions(req.n)
    sorted_data = merge_sort(data)
    q3 = get_q3_sales(sorted_data, req.q3_start, req.q3_end)
    return {
        "n":        req.n,
        "q3_start": req.q3_start,
        "q3_end":   req.q3_end,
        "matches":  len(q3),
        "preview":  q3[:10],
    }


# ─────────────────────────────────────────────
# Phase 2 — Applied Steps Tracker
# ─────────────────────────────────────────────

def _tracker_state() -> dict:
    """Serialises the doubly linked list into a JSON-friendly snapshot."""
    steps, node = [], _tracker.head
    while node:
        steps.append({
            "step":    node.step,
            "current": node is _tracker.current,
        })
        node = node.next
    return {"steps": steps, "current": _tracker.current.step if _tracker.current else None}


@app.post("/tracker/add", tags=["Phase 2 — Steps Tracker"])
def tracker_add(req: StepRequest) -> dict:
    """Adds a new ETL transformation step to the doubly linked list."""
    _tracker.add_step(req.step_name)
    return _tracker_state()


@app.post("/tracker/undo", tags=["Phase 2 — Steps Tracker"])
def tracker_undo() -> dict:
    """Moves the current pointer one step backward — O(1)."""
    _tracker.undo()
    return _tracker_state()


@app.post("/tracker/redo", tags=["Phase 2 — Steps Tracker"])
def tracker_redo() -> dict:
    """Moves the current pointer one step forward — O(1)."""
    _tracker.redo()
    return _tracker_state()


@app.get("/tracker/state", tags=["Phase 2 — Steps Tracker"])
def tracker_state() -> dict:
    """Returns the full history and active step."""
    return _tracker_state()


@app.delete("/tracker/reset", tags=["Phase 2 — Steps Tracker"])
def tracker_reset() -> dict:
    """Clears the entire transformation history."""
    global _tracker
    _tracker = AdvancedTracker()
    return {"message": "Tracker reset.", "steps": []}


# ─────────────────────────────────────────────
# Phase 3 — DAX Formula Parser
# ─────────────────────────────────────────────

@app.post("/dax/evaluate", tags=["Phase 3 — DAX Parser"])
def dax_evaluate(req: DAXRequest) -> dict:
    """
    Converts an infix expression to postfix (Shunting-Yard),
    then evaluates it using the stack-based evaluator.
    """
    try:
        postfix = _evaluator._converter.convert(req.expression)
        result  = _evaluator.evaluate_postfix(postfix)
        return {
            "infix":   req.expression,
            "postfix": postfix,
            "result":  result,
        }
    except ZeroDivisionError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=422, detail=str(e))


@app.post("/dax/evaluate-postfix", tags=["Phase 3 — DAX Parser"])
def dax_evaluate_postfix(req: PostfixRequest) -> dict:
    """Evaluates a postfix (RPN) expression directly using the stack evaluator."""
    try:
        result = _evaluator.evaluate_postfix(req.expression)
        return {"postfix": req.expression, "result": result}
    except Exception as e:
        raise HTTPException(status_code=422, detail=str(e))


# ─────────────────────────────────────────────
# Phase 4 — Live Data Buffer
# ─────────────────────────────────────────────

@app.post("/buffer/enqueue", tags=["Phase 4 — Live Buffer"])
def buffer_enqueue(row: TransactionRow) -> dict:
    """Enqueues a new POS transaction row into the live buffer — O(1)."""
    _buffer.enqueue_row(row.model_dump())
    return {"queued": row.model_dump(), "queue_depth": _buffer.size()}


@app.post("/buffer/process", tags=["Phase 4 — Live Buffer"])
def buffer_process(req: BatchRequest) -> dict:
    """Dequeues up to batch_size rows and 'pushes' them to Power BI — O(k)."""
    batch = _buffer.process_batch(req.batch_size)
    return {
        "processed":   batch,
        "count":       len(batch),
        "remaining":   _buffer.size(),
    }


@app.get("/buffer/status", tags=["Phase 4 — Live Buffer"])
def buffer_status() -> dict:
    """Returns the current queue depth."""
    return {"queue_depth": _buffer.size(), "is_empty": _buffer.is_empty()}


@app.delete("/buffer/flush", tags=["Phase 4 — Live Buffer"])
def buffer_flush() -> dict:
    """Drains the entire buffer and returns all rows."""
    global _buffer
    all_rows = _buffer.process_batch(_buffer.size() or 1)
    _buffer = LiveIngestionQueue()
    return {"flushed": all_rows, "count": len(all_rows)}


# ─────────────────────────────────────────────
# Phase 5 — Trees: BST + Org Chart
# ─────────────────────────────────────────────

@app.post("/bst/insert", tags=["Phase 5 — Trees"])
def bst_insert(req: CustomerRequest) -> dict:
    """Inserts a customer into the BST Dimension Index — O(log n)."""
    _dim_index.insert(req.national_id, req.name)
    return {
        "inserted":    {"national_id": req.national_id, "name": req.name},
        "total_nodes": len(_dim_index),
    }


@app.get("/bst/search/{national_id}", tags=["Phase 5 — Trees"])
def bst_search(national_id: int) -> dict:
    """Searches the BST for a customer by National ID — O(log n)."""
    name = _dim_index.search(national_id)
    if name is None:
        raise HTTPException(status_code=404, detail=f"Customer {national_id} not found in index.")
    return {"national_id": national_id, "name": name}


@app.get("/bst/all", tags=["Phase 5 — Trees"])
def bst_all() -> dict:
    """Returns all customers in ascending National ID order (BST in-order traversal)."""
    records = [{"national_id": nid, "name": name} for nid, name in _dim_index.in_order()]
    return {"count": len(records), "customers": records}


@app.delete("/bst/reset", tags=["Phase 5 — Trees"])
def bst_reset() -> dict:
    """Clears the BST dimension index."""
    global _dim_index
    _dim_index = DimensionIndex()
    return {"message": "BST reset.", "total_nodes": 0}


@app.get("/org-chart", tags=["Phase 5 — Trees"])
def org_chart() -> dict:
    """Returns the NileMart organisational hierarchy as a nested JSON tree."""

    def _serialize(node) -> dict:
        return {
            "name":     node.name,
            "sales":    node.sales,
            "total_sales": _org_chart.roll_up_sales(node),
            "children": [_serialize(c) for c in node.children],
        }

    return {"org_chart": _serialize(_org_chart.ceo)}


@app.get("/org-chart/rollup/{manager_name}", tags=["Phase 5 — Trees"])
def org_rollup(manager_name: str) -> dict:
    """
    Recursively rolls up sales for a named manager node.
    Valid names: 'ceo', 'vp_cairo', 'vp_alex'
    """
    node_map = {
        "ceo":      _org_chart.ceo,
        "vp_cairo": _org_chart.vp_cairo,
        "vp_alex":  _org_chart.vp_alex,
    }
    node = node_map.get(manager_name.lower())
    if node is None:
        raise HTTPException(
            status_code=404,
            detail=f"Manager '{manager_name}' not found. Valid keys: {list(node_map.keys())}",
        )
    total = _org_chart.roll_up_sales(node)
    return {"manager": node.name, "total_sales_egp": total}
