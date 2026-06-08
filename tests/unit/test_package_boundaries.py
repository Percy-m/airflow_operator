from __future__ import annotations

import importlib
import sys
from contextlib import contextmanager
from importlib.abc import MetaPathFinder
from pathlib import Path
from types import ModuleType


PROJECT_ROOT = Path(__file__).resolve().parents[2]
SRC_ROOT = PROJECT_ROOT / "src" / "custom_operator"


class BlockPackageFinder(MetaPathFinder):
    def __init__(self, blocked_prefixes: set[str]) -> None:
        self.blocked_prefixes = blocked_prefixes

    def find_spec(self, fullname, path=None, target=None):
        if fullname in self.blocked_prefixes or any(
            fullname.startswith(f"{prefix}.") for prefix in self.blocked_prefixes
        ):
            raise ModuleNotFoundError(f"Blocked import: {fullname}")
        return None


@contextmanager
def block_imports(*blocked_prefixes: str):
    finder = BlockPackageFinder(set(blocked_prefixes))
    saved_modules: dict[str, ModuleType] = {}
    prefixes = (*blocked_prefixes, "custom_operator.spark", "custom_operator.ray")
    for name in list(sys.modules):
        if name in prefixes or any(name.startswith(f"{prefix}.") for prefix in prefixes):
            saved_modules[name] = sys.modules.pop(name)

    sys.meta_path.insert(0, finder)
    try:
        yield
    finally:
        sys.meta_path.remove(finder)
        for name in list(sys.modules):
            if name in prefixes or any(name.startswith(f"{prefix}.") for prefix in prefixes):
                sys.modules.pop(name)
        sys.modules.update(saved_modules)


def test_spark_package_imports_without_token_or_ray_packages():
    with block_imports("custom_operator.token", "custom_operator.ray"):
        for module_name in (
            "custom_operator.spark.clients.spark_api",
            "custom_operator.spark.hooks.spark",
            "custom_operator.spark.triggers.spark",
            "custom_operator.spark.operators.spark",
        ):
            importlib.import_module(module_name)


def test_ray_package_imports_without_token_or_spark_packages():
    with block_imports("custom_operator.token", "custom_operator.spark"):
        for module_name in (
            "custom_operator.ray.clients.ray_api",
            "custom_operator.ray.hooks.ray",
            "custom_operator.ray.triggers.ray",
            "custom_operator.ray.operators.ray",
        ):
            importlib.import_module(module_name)


def test_spark_and_ray_source_do_not_import_other_runtime_packages():
    forbidden_by_package = {
        "spark": ("custom_operator.token", "custom_operator.ray"),
        "ray": ("custom_operator.token", "custom_operator.spark"),
    }
    violations: list[str] = []

    for package, forbidden_imports in forbidden_by_package.items():
        for path in (SRC_ROOT / package).rglob("*.py"):
            text = path.read_text()
            for forbidden_import in forbidden_imports:
                if forbidden_import in text:
                    violations.append(f"{path.relative_to(PROJECT_ROOT)} imports {forbidden_import}")

    assert violations == []
