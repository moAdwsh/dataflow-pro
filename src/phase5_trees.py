"""
DataFlow Pro - Phase 5: The Hierarchical Matrix Builder
=======================================================
Business Goal: Model complex Parent-Child relationships for Power BI
               matrix visuals and drill-down reports.

Implements:
  - BSTNode / DimensionIndex  → Binary Search Tree for Customer IDs
  - OrgChartAnalyzer          → N-ary org tree via anytree + recursive roll-up
"""

from anytree import Node, RenderTree


# ─────────────────────────────────────────────
# Part 1: Binary Search Tree — Dimension Index
# ─────────────────────────────────────────────

class BSTNode:
    """A single node in the Customer ID BST."""
    def __init__(self, national_id: int, name: str):
        self.national_id: int = national_id
        self.name:        str = name
        self.left:  "BSTNode | None" = None
        self.right: "BSTNode | None" = None


class DimensionIndex:
    """
    Binary Search Tree that stores unique Customer IDs (National IDs).
    Mirrors how Power BI compresses dimension tables for O(log n) relationships.

    Time complexity (balanced):
      insert : O(log n)
      search : O(log n)
    """

    def __init__(self):
        self.root: BSTNode | None = None

    # ── Insert ──────────────────────────────

    def insert(self, national_id: int, name: str) -> None:
        """Inserts a customer record; silently ignores duplicate IDs."""
        self.root = self._insert(self.root, national_id, name)

    def _insert(self, node: BSTNode | None, nid: int, name: str) -> BSTNode:
        if node is None:
            return BSTNode(nid, name)
        if nid < node.national_id:
            node.left  = self._insert(node.left,  nid, name)
        elif nid > node.national_id:
            node.right = self._insert(node.right, nid, name)
        # Equal → duplicate; ignore
        return node

    # ── Search ──────────────────────────────

    def search(self, national_id: int) -> str | None:
        """
        Returns the customer name for a given national_id, or None if not found.
        O(log n) average — same Big-O as Binary Search on a sorted array, but
        no re-sorting required after inserts.
        """
        node = self._search(self.root, national_id)
        return node.name if node else None

    def _search(self, node: BSTNode | None, nid: int) -> BSTNode | None:
        if node is None or node.national_id == nid:
            return node
        if nid < node.national_id:
            return self._search(node.left, nid)
        return self._search(node.right, nid)

    # ── In-order traversal (sorted output) ──

    def in_order(self) -> list[tuple[int, str]]:
        """Returns all records in ascending national_id order."""
        result: list[tuple[int, str]] = []
        self._in_order(self.root, result)
        return result

    def _in_order(self, node: BSTNode | None, result: list) -> None:
        if node is None:
            return
        self._in_order(node.left, result)
        result.append((node.national_id, node.name))
        self._in_order(node.right, result)

    def __len__(self) -> int:
        return len(self.in_order())


# ─────────────────────────────────────────────
# Part 2: N-ary Tree — Organizational Chart
# ─────────────────────────────────────────────

class OrgChartAnalyzer:
    """
    Uses anytree to model NileMart's corporate hierarchy:

        Omar (Global CEO)
        ├── Tarek (VP Cairo & Giza)
        │   ├── Aya   (Maadi Branch Rep)    — 150,000 EGP
        │   └── Mahmoud (Zayed Branch Rep)  — 270,000 EGP
        └── Salma (VP Alex & Delta)
            ├── Kareem (Smouha Branch Rep)  — 180,000 EGP
            └── Nour   (Mansoura Branch Rep)— 120,000 EGP

    The roll_up_sales() function traverses the tree bottom-up to aggregate
    revenue, simulating Power BI's SUMX / parent-child DAX patterns.
    """

    def __init__(self):
        # ── Corporate hierarchy ──────────────────
        self.ceo      = Node("Omar (Global CEO)",         sales=0)
        self.vp_cairo = Node("Tarek (VP Cairo & Giza)",   sales=0,       parent=self.ceo)
        self.vp_alex  = Node("Salma (VP Alex & Delta)",   sales=0,       parent=self.ceo)

        # Leaf nodes — actual revenue generators
        Node("Aya (Maadi Branch Rep)",      sales=150_000, parent=self.vp_cairo)
        Node("Mahmoud (Zayed Branch Rep)",  sales=270_000, parent=self.vp_cairo)
        Node("Kareem (Smouha Branch Rep)",  sales=180_000, parent=self.vp_alex)
        Node("Nour (Mansoura Branch Rep)",  sales=120_000, parent=self.vp_alex)

    def display_chart(self) -> None:
        """Prints the visual org-chart hierarchy to the console."""
        print("\n[Org Chart] NileMart Hierarchy:")
        for pre, _, node in RenderTree(self.ceo):
            print(f"  {pre}{node.name}  (Direct Sales: {node.sales:>10,} EGP)")

    def roll_up_sales(self, node: Node) -> int:
        """
        Recursively sums a manager's own sales PLUS the sales of every
        descendant.  Time complexity: O(n) — visits each node exactly once.

        This mirrors Power BI's PATH / SUMX patterns for parent-child hierarchies.

        Args:
            node: any anytree Node (typically a VP or the CEO)

        Returns:
            Total revenue (EGP) attributable to this node's subtree.
        """
        return node.sales + sum(self.roll_up_sales(child) for child in node.children)

    def print_roll_up_report(self) -> None:
        """Prints a full revenue roll-up report for every manager."""
        print("\n[Roll-Up]  Revenue Attribution Report:")
        print(f"{'─'*50}")
        for node in [self.vp_cairo, self.vp_alex, self.ceo]:
            total = self.roll_up_sales(node)
            print(f"  {node.name:<35} → {total:>12,} EGP")
        print(f"{'─'*50}")


# ─────────────────────────────────────────────
# Demo / Self-test
# ─────────────────────────────────────────────

def demo() -> None:
    print("\n========== Phase 5: Hierarchical Matrix Builder ==========")

    # --- BST: Dimension Index ---
    print("\n-- Binary Search Tree (Customer Dimension Index) --")
    dim = DimensionIndex()
    customers = [
        (29050112345678, "Aya Hassan"),
        (30060223456789, "Mahmoud Ali"),
        (28040334567890, "Kareem Samy"),
        (31070445678901, "Nour Tarek"),
        (27030556789012, "Omar Walid"),
    ]
    for nid, name in customers:
        dim.insert(nid, name)

    print(f"[BST]  Indexed {len(dim)} customers.")
    search_id = customers[1][0]
    result = dim.search(search_id)
    print(f"[BST]  Search {search_id} → '{result}'")

    print("[BST]  In-order (sorted by National ID):")
    for nid, name in dim.in_order():
        print(f"         {nid}  →  {name}")

    # --- N-ary Tree: Org Chart & Roll-Up ---
    print("\n-- N-ary Organizational Tree --")
    org = OrgChartAnalyzer()
    org.display_chart()
    org.print_roll_up_report()


if __name__ == "__main__":
    demo()
