"""
DataFlow Pro - Phase 3: The DAX Formula Parser
===============================================
Business Goal: Evaluate custom KPI formulas on the fly.

Implements:
  - ArrayStack         → stack backed by a Python list
  - LinkedListStack    → stack backed by a singly linked list
  - DAXEvaluator       → postfix (RPN) expression evaluator using ArrayStack
  - InfixToPostfix     → Shunting-Yard algorithm to convert human-readable
                         infix expressions to postfix before evaluation
"""


# ─────────────────────────────────────────────
# Stack — Array Representation
# ─────────────────────────────────────────────

class ArrayStack:
    """
    Stack implemented with a Python list.
    All operations are O(1) amortised.
    Used as the default backing store for DAXEvaluator.
    """

    def __init__(self):
        self._data: list = []

    def push(self, value) -> None:
        self._data.append(value)

    def pop(self):
        if self.is_empty():
            raise IndexError("pop from empty stack")
        return self._data.pop()

    def peek(self):
        if self.is_empty():
            return None
        return self._data[-1]

    def is_empty(self) -> bool:
        return len(self._data) == 0

    def __len__(self) -> int:
        return len(self._data)

    def __repr__(self) -> str:
        return f"ArrayStack({self._data})"


# ─────────────────────────────────────────────
# Stack — Linked List Representation
# ─────────────────────────────────────────────

class _SLLNode:
    def __init__(self, data):
        self.data = data
        self.next: "_SLLNode | None" = None


class LinkedListStack:
    """
    Stack implemented with a singly linked list.
    All operations are O(1) — no array resizing overhead.
    Demonstrates the alternative internal representation.
    """

    def __init__(self):
        self._top: _SLLNode | None = None
        self._size: int = 0

    def push(self, value) -> None:
        node = _SLLNode(value)
        node.next = self._top
        self._top = node
        self._size += 1

    def pop(self):
        if self.is_empty():
            raise IndexError("pop from empty stack")
        value = self._top.data
        self._top = self._top.next
        self._size -= 1
        return value

    def peek(self):
        return self._top.data if self._top else None

    def is_empty(self) -> bool:
        return self._top is None

    def __len__(self) -> int:
        return self._size

    def __repr__(self) -> str:
        items, node = [], self._top
        while node:
            items.append(node.data)
            node = node.next
        return f"LinkedListStack({list(reversed(items))})"


# ─────────────────────────────────────────────
# Infix → Postfix Converter  (Shunting-Yard)
# ─────────────────────────────────────────────

_PRECEDENCE = {"+": 1, "-": 1, "*": 2, "/": 2, "^": 3}
_RIGHT_ASSOC = {"^"}


class InfixToPostfix:
    """
    Converts a human-readable infix DAX expression to postfix (RPN)
    using Dijkstra's Shunting-Yard algorithm — O(n) time, O(n) space.

    Example:
        "(15000 + 5000) * 2"  →  "15000 5000 + 2 *"
    """

    def convert(self, expression: str) -> str:
        output: list[str] = []
        op_stack = ArrayStack()

        tokens = expression.replace("(", " ( ").replace(")", " ) ").split()

        for token in tokens:
            if self._is_number(token):
                output.append(token)
            elif token == "(":
                op_stack.push(token)
            elif token == ")":
                while not op_stack.is_empty() and op_stack.peek() != "(":
                    output.append(op_stack.pop())
                if op_stack.is_empty():
                    raise ValueError("Mismatched parentheses — check your DAX formula.")
                op_stack.pop()  # discard "("
            elif token in _PRECEDENCE:
                while (
                    not op_stack.is_empty()
                    and op_stack.peek() in _PRECEDENCE
                    and (
                        _PRECEDENCE[op_stack.peek()] > _PRECEDENCE[token]
                        or (
                            _PRECEDENCE[op_stack.peek()] == _PRECEDENCE[token]
                            and token not in _RIGHT_ASSOC
                        )
                    )
                ):
                    output.append(op_stack.pop())
                op_stack.push(token)
            else:
                raise ValueError(f"Unknown token in DAX expression: '{token}'")

        while not op_stack.is_empty():
            top = op_stack.pop()
            if top == "(":
                raise ValueError("Mismatched parentheses — check your DAX formula.")
            output.append(top)

        return " ".join(output)

    @staticmethod
    def _is_number(token: str) -> bool:
        try:
            float(token)
            return True
        except ValueError:
            return False


# ─────────────────────────────────────────────
# DAX Postfix Evaluator
# ─────────────────────────────────────────────

class DAXEvaluator:
    """
    Evaluates postfix (RPN) mathematical expressions using a stack.

    Supports: + - * / ^ and floating-point operands.
    Time complexity: O(n) — single pass through the token list.

    Usage:
        evaluator = DAXEvaluator()
        result = evaluator.evaluate_postfix("15000 5000 + 2 *")  # → 40000
        result = evaluator.evaluate("(15000 + 5000) * 2")        # infix shortcut
    """

    def __init__(self):
        self._converter = InfixToPostfix()

    def evaluate_postfix(self, expression: str) -> float:
        """Evaluates a postfix (RPN) expression string."""
        stack = ArrayStack()

        for token in expression.split():
            if self._is_number(token):
                stack.push(float(token))
            elif token in {"+", "-", "*", "/", "^"}:
                if len(stack) < 2:
                    raise ValueError("Invalid postfix expression — insufficient operands.")
                b = stack.pop()
                a = stack.pop()
                stack.push(self._apply(a, token, b))
            else:
                raise ValueError(f"Unknown token: '{token}'")

        if len(stack) != 1:
            raise ValueError("Invalid postfix expression — too many operands.")
        return stack.pop()

    def evaluate(self, infix_expression: str) -> float:
        """Converts infix to postfix, then evaluates. Convenience method."""
        postfix = self._converter.convert(infix_expression)
        print(f"[Parser]   Infix  : {infix_expression}")
        print(f"[Parser]   Postfix: {postfix}")
        return self.evaluate_postfix(postfix)

    @staticmethod
    def _apply(a: float, op: str, b: float) -> float:
        if op == "+": return a + b
        if op == "-": return a - b
        if op == "*": return a * b
        if op == "/":
            if b == 0:
                raise ZeroDivisionError("DAX Error: Division by zero in formula.")
            return a / b
        if op == "^": return a ** b
        raise ValueError(f"Unsupported operator: '{op}'")

    @staticmethod
    def _is_number(token: str) -> bool:
        try:
            float(token)
            return True
        except ValueError:
            return False


# ─────────────────────────────────────────────
# Demo / Self-test
# ─────────────────────────────────────────────

def demo() -> None:
    print("\n========== Phase 3: DAX Formula Parser ==========")
    evaluator = DAXEvaluator()

    tests = [
        # (label, infix_expression, expected)
        ("Revenue KPI",       "(15000 + 5000) * 2",       40_000),
        ("Profit Margin",     "5000 * 5000 + 2",          25_000_002),
        ("Tax Calculation",   "(100000 - 20000) / 4",     20_000),
        ("Compound formula",  "(3 + 5) * (2 ^ 3) - 4",   60),
    ]

    for label, expr, expected in tests:
        result = evaluator.evaluate(expr)
        status = "✓" if abs(result - expected) < 1e-6 else "✗"
        print(f"  [{status}] {label}: {result:,.2f}  (expected {expected:,.2f})\n")

    # Postfix directly (e.g., formula already converted upstream)
    print("[Parser]   Direct postfix evaluation:")
    result = evaluator.evaluate_postfix("15000 5000 + 2 *")
    print(f"  '15000 5000 + 2 *'  →  {result:,.0f}\n")


if __name__ == "__main__":
    demo()
