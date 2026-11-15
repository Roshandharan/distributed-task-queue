"""Task used by the crash test: does a little work and records that it ran."""

from __future__ import annotations

import os
import random
import threading
import time

import psycopg

from taskq import task
from taskq.config import load

_local = threading.local()


def _conn() -> psycopg.Connection:
    if getattr(_local, "conn", None) is None:
        _local.conn = psycopg.connect(load().database_url, autocommit=True)
    return _local.conn


@task("chaos_work")
def chaos_work(n: int) -> int:
    time.sleep(random.uniform(0.02, 0.06))
    _conn().execute("INSERT INTO chaos_effects (n, pid) VALUES (%s, %s)", (n, os.getpid()))
    return n
