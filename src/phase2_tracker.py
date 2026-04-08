"""
DataFlow Pro - Phase 2: The "Applied Steps" Tracker
=====================================================
Business Goal: Recreate Power Query's transformation tracking system.

Implements:
  - Singly Linked List  → AppliedStepsTracker  (append-only step history)
  - Doubly Linked List  → AdvancedTracker       (O(1) undo / redo navigation)
"""


# ─────────────────────────────────────────────
# Singly Linked List — Step History
# ─────────────────────────────────────────────

class StepNode:
    """A single node in a singly linked list representing one ETL transformation."""
    def __init__(self, step_name: str):
        self.step: str = step_name
        self.next: "StepNode | None" = None


class AppliedStepsTracker:
    """
    Singly Linked List that stores the ordered history of Power Query–style
    transformation steps (e.g., Remove Nulls → Change Type → Filter Rows).

    Time complexity:
      add_step  : O(n) — traverses to tail; acceptable because step count is small
      show_steps: O(n)
    """

    def __init__(self):
        self.head: StepNode | None = None

    def add_step(self, step_name: str) -> None:
        """Appends a new transformation step to the end of the history."""
        new_node = StepNode(step_name)
        if not self.head:
            self.head = new_node
        else:
            current = self.head
            while current.next:
                current = current.next
            current.next = new_node
        print(f"[Tracker]  Added step: '{step_name}'")

    def show_steps(self) -> None:
        """Prints the full transformation pipeline in order."""
        if not self.head:
            print("[Tracker]  No steps recorded yet.")
            return
        current = self.head
        print("\n[Tracker]  Applied Steps:")
        step_num = 1
        while current:
            print(f"  {step_num}. {current.step}")
            current = current.next
            step_num += 1

    def __len__(self) -> int:
        count, current = 0, self.head
        while current:
            count += 1
            current = current.next
        return count


# ─────────────────────────────────────────────
# Doubly Linked List — Undo / Redo Engine
# ─────────────────────────────────────────────

class DoublyStepNode:
    """A node in a doubly linked list — holds both forward and backward pointers."""
    def __init__(self, step_name: str):
        self.step: str = step_name
        self.next: "DoublyStepNode | None" = None
        self.prev: "DoublyStepNode | None" = None


class AdvancedTracker:
    """
    Doubly Linked List that supports O(1) undo and redo navigation through
    the transformation history — mirrors Power BI's Applied Steps panel.

    Time complexity:
      add_step    : O(1) — always appends at current position
      undo / redo : O(1) — pointer move only; no data is reloaded
    """

    def __init__(self):
        self.head:    DoublyStepNode | None = None
        self.current: DoublyStepNode | None = None

    def add_step(self, step_name: str) -> None:
        """
        Appends a new step after the current position.
        Any 'future' steps beyond current are discarded (matches Power Query behaviour).
        """
        new_node = DoublyStepNode(step_name)
        if not self.head:
            self.head = new_node
            self.current = new_node
        else:
            # Sever any redo branch — new action overwrites future
            self.current.next = new_node
            new_node.prev = self.current
            self.current = new_node
        print(f"[Tracker]  Added: '{step_name}'")

    def undo(self) -> None:
        """Steps backward in history — O(1)."""
        if self.current and self.current.prev:
            self.current = self.current.prev
            print(f"[Undo]     ← Current step: '{self.current.step}'")
        else:
            print("[Undo]     Nothing to undo — already at the beginning.")

    def redo(self) -> None:
        """Steps forward in history — O(1)."""
        if self.current and self.current.next:
            self.current = self.current.next
            print(f"[Redo]     → Current step: '{self.current.step}'")
        else:
            print("[Redo]     Nothing to redo — already at the latest step.")

    def show_current(self) -> None:
        """Displays the currently active transformation step."""
        if self.current:
            print(f"[Current]  ▶ '{self.current.step}'")
        else:
            print("[Current]  No steps recorded yet.")

    def show_all(self) -> None:
        """Prints the full history, highlighting the active step."""
        if not self.head:
            print("[Tracker]  No steps recorded yet.")
            return
        node = self.head
        print("\n[Tracker]  Full History:")
        while node:
            marker = " ◀ CURRENT" if node is self.current else ""
            print(f"  - {node.step}{marker}")
            node = node.next


# ─────────────────────────────────────────────
# Demo / Self-test
# ─────────────────────────────────────────────

def demo() -> None:
    print("\n========== Phase 2: Applied Steps Tracker ==========")

    # --- Singly Linked List ---
    print("\n-- Singly Linked List (append-only) --")
    tracker = AppliedStepsTracker()
    tracker.add_step("Remove Nulls")
    tracker.add_step("Change Type")
    tracker.add_step("Filter Rows")
    tracker.show_steps()

    # --- Doubly Linked List with Undo/Redo ---
    print("\n-- Doubly Linked List (Undo/Redo engine) --")
    adv = AdvancedTracker()
    adv.add_step("Remove Nulls")
    adv.add_step("Change Type")
    adv.add_step("Filter Rows")

    adv.show_all()
    adv.undo()
    adv.undo()
    adv.redo()
    adv.show_current()
    adv.show_all()


if __name__ == "__main__":
    demo()
