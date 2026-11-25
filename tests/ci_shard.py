"""Split the CI test matrix by module while preserving pytest collection order."""

from collections import Counter

import pytest


def pytest_addoption(parser):
    parser.addoption("--ci-shard", nargs=2, type=int, metavar=("INDEX", "TOTAL"), help="Run one CI shard.")


def pytest_collection_modifyitems(config, items):
    shard = config.getoption("--ci-shard")
    if shard is None:
        return
    index, total = shard
    if not 1 <= index <= total:
        raise pytest.UsageError("--ci-shard requires 1 <= INDEX <= TOTAL")

    counts = Counter(item.path for item in items)
    assignments = {}
    loads = [0] * total
    for path in sorted(counts, key=lambda path: (-counts[path], path.as_posix())):
        slot = min(range(total), key=loads.__getitem__)
        assignments[path] = slot
        loads[slot] += counts[path]

    deselected = [item for item in items if assignments[item.path] != index - 1]
    items[:] = [item for item in items if assignments[item.path] == index - 1]
    config.hook.pytest_deselected(items=deselected)
