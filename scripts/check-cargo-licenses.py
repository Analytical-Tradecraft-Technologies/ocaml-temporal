#!/usr/bin/env python3
"""Fail closed when locked Cargo metadata contains an unapproved license."""

from __future__ import annotations

import argparse
import json
import re
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any, TextIO


ALLOWED_LICENSES = {
    "0BSD",
    "Apache-2.0",
    "BSD-2-Clause",
    "BSD-3-Clause",
    "CC0-1.0",
    "CDLA-Permissive-2.0",
    "ISC",
    "MIT",
    "MIT-0",
    "PostgreSQL",
    "Unicode-3.0",
    "Unlicense",
    "Zlib",
}
ALLOWED_WITH_EXCEPTIONS = {"Apache-2.0 WITH LLVM-exception"}
CORE_REVISION = "95e97686a079dcfe6c42e3254b2f3f5e3d97408f"
CORE_SOURCE_PREFIX = (
    "git+https://github.com/temporalio/sdk-core.git?rev="
    f"{CORE_REVISION}#"
)
CORE_PACKAGES = {
    "temporalio-client",
    "temporalio-common",
    "temporalio-common-wasm",
    "temporalio-macros",
    "temporalio-protos",
    "temporalio-sdk-core",
}
# The reviewed licence of the pinned Core workspace `LICENSE.txt`. Cargo
# metadata carries only the file path for these packages, so this constant is
# the single conclusion shared by the policy scanner, the SBOM generator, and
# the third-party notices generator.
REVIEWED_CORE_LICENSE = "MIT"


class ExpressionError(ValueError):
    """Raised for malformed SPDX-like Cargo license metadata."""


@dataclass(frozen=True)
class Node:
    kind: str
    value: str | None = None
    left: "Node | None" = None
    right: "Node | None" = None


def normalize_expression(expression: str) -> str:
    """Rewrite Cargo's historical `MIT/Apache-2.0` spelling as SPDX `OR`.

    Whitespace is also collapsed so equivalent spellings produce one string.
    The result is not validated; parse it with `Parser` when that matters.
    """

    normalized = re.sub(r"(?<=\S)\s*/\s*(?=\S)", " OR ", expression)
    return " ".join(normalized.split())


def tokenize(expression: str) -> list[str]:
    """Split one licence expression into identifiers, operators, and parentheses."""

    tokens = re.findall(r"\(|\)|[^\s()]+", normalize_expression(expression))
    if not tokens:
        raise ExpressionError("empty license expression")
    return tokens


class Parser:
    def __init__(self, expression: str) -> None:
        self.tokens = tokenize(expression)
        self.position = 0

    def parse(self) -> Node:
        node = self.parse_or()
        if self.position != len(self.tokens):
            raise ExpressionError(f"unexpected token {self.peek()!r}")
        return node

    def peek(self) -> str | None:
        if self.position == len(self.tokens):
            return None
        return self.tokens[self.position]

    def take(self) -> str:
        token = self.peek()
        if token is None:
            raise ExpressionError("unexpected end of expression")
        self.position += 1
        return token

    def parse_or(self) -> Node:
        node = self.parse_and()
        while self.peek() == "OR":
            self.take()
            node = Node("or", left=node, right=self.parse_and())
        return node

    def parse_and(self) -> Node:
        node = self.parse_with()
        while self.peek() == "AND":
            self.take()
            node = Node("and", left=node, right=self.parse_with())
        return node

    def parse_with(self) -> Node:
        node = self.parse_primary()
        if self.peek() == "WITH":
            self.take()
            exception = self.take()
            if exception in {"AND", "OR", "WITH", "(", ")"}:
                raise ExpressionError("expected an exception identifier after WITH")
            node = Node("with", value=exception, left=node)
        return node

    def parse_primary(self) -> Node:
        token = self.take()
        if token == "(":
            node = self.parse_or()
            if self.take() != ")":
                raise ExpressionError("expected closing parenthesis")
            return node
        if token in {"AND", "OR", "WITH", ")"}:
            raise ExpressionError(f"unexpected token {token!r}")
        return Node("license", value=token)


def evaluate(node: Node) -> tuple[bool, str]:
    if node.kind == "license":
        assert node.value is not None
        return node.value in ALLOWED_LICENSES, node.value
    if node.kind == "with":
        assert node.left is not None and node.value is not None
        if node.left.kind != "license" or node.left.value is None:
            return False, "invalid WITH operand"
        expression = f"{node.left.value} WITH {node.value}"
        return expression in ALLOWED_WITH_EXCEPTIONS, expression
    if node.kind == "and":
        assert node.left is not None and node.right is not None
        left_allowed, left_choice = evaluate(node.left)
        right_allowed, right_choice = evaluate(node.right)
        return left_allowed and right_allowed, f"{left_choice} AND {right_choice}"
    if node.kind == "or":
        assert node.left is not None and node.right is not None
        left_allowed, left_choice = evaluate(node.left)
        if left_allowed:
            return True, left_choice
        return evaluate(node.right)
    raise AssertionError(f"unknown node kind {node.kind}")


def reviewed_core_license(package: dict[str, Any]) -> bool:
    """Recognize the exact pinned Core packages whose licence is a file."""

    source = package.get("source") or ""
    license_file = package.get("license_file") or ""
    return (
        package.get("name") in CORE_PACKAGES
        and source.startswith(CORE_SOURCE_PREFIX)
        and Path(license_file).name == "LICENSE.txt"
    )


def declared_license(package: dict[str, Any]) -> str | None:
    """Return the package's own Cargo licence expression in SPDX spelling.

    `None` means the package declares no expression, or one that cannot be
    parsed; policy approval is deliberately not considered here.
    """

    expression = package.get("license")
    if not isinstance(expression, str):
        return None
    try:
        Parser(expression).parse()
    except ExpressionError:
        return None
    return normalize_expression(expression)


def concluded_license(package: dict[str, Any]) -> str | None:
    """Return the licence this repository concludes for one Cargo package.

    A declared expression is used as-is. A package without one is concluded
    only for the reviewed Core workspace file; every other package yields
    `None` so callers record NOASSERTION or fail instead of guessing.
    """

    declared = declared_license(package)
    if declared is not None:
        return declared
    if package.get("license") is None and reviewed_core_license(package):
        return REVIEWED_CORE_LICENSE
    return None


def check(metadata: dict[str, Any], output: TextIO) -> bool:
    packages = metadata.get("packages")
    if not isinstance(packages, list):
        print("DENY metadata missing packages array", file=output)
        return False

    passed = True
    for package in sorted(packages, key=lambda item: (item.get("name", ""), item.get("version", ""))):
        name = package.get("name", "<missing-name>")
        version = package.get("version", "<missing-version>")
        expression = package.get("license")
        if expression is None:
            if reviewed_core_license(package):
                print(
                    f"ALLOW {name} {version} {REVIEWED_CORE_LICENSE} via pinned Core LICENSE.txt",
                    file=output,
                )
            else:
                print(f"DENY  {name} {version} missing-license", file=output)
                passed = False
            continue
        if not isinstance(expression, str):
            print(f"DENY  {name} {version} invalid-license-value", file=output)
            passed = False
            continue
        try:
            allowed, choice = evaluate(Parser(expression).parse())
        except ExpressionError as error:
            print(f"DENY  {name} {version} malformed-expression: {error}", file=output)
            passed = False
            continue
        if allowed:
            print(f"ALLOW {name} {version} {expression} via {choice}", file=output)
        else:
            print(f"DENY  {name} {version} {expression}", file=output)
            passed = False
    return passed


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--metadata", required=True, help="Cargo metadata JSON path, or - for stdin")
    arguments = parser.parse_args()
    stream = sys.stdin if arguments.metadata == "-" else open(arguments.metadata, encoding="utf-8")
    try:
        metadata = json.load(stream)
    except (OSError, json.JSONDecodeError) as error:
        print(f"DENY invalid Cargo metadata: {error}", file=sys.stderr)
        return 1
    finally:
        if stream is not sys.stdin:
            stream.close()
    return 0 if check(metadata, sys.stdout) else 1


if __name__ == "__main__":
    raise SystemExit(main())
