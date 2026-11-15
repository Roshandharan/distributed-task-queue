"""Task registry: maps task names to Python callables."""

from __future__ import annotations

import importlib
from collections.abc import Callable
from typing import Any

_TASKS: dict[str, Callable[..., Any]] = {}


def task(name: str | None = None) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
    """Register a function as a task. The function receives the job payload as kwargs."""

    def decorator(fn: Callable[..., Any]) -> Callable[..., Any]:
        _TASKS[name or fn.__name__] = fn
        return fn

    return decorator


def get(name: str) -> Callable[..., Any]:
    try:
        return _TASKS[name]
    except KeyError:
        raise UnknownTask(name) from None


def load_modules(modules: list[str]) -> None:
    for module in modules:
        importlib.import_module(module)


def names() -> list[str]:
    return sorted(_TASKS)


class UnknownTask(Exception):
    """Raised when a job names a task no worker has registered."""
