"""Independent reference implementation of the impact-propagation algorithm.

Deliberately shares no code with the product planner (planner/). Given a graph
definition and dirty seeds it outputs only the dirty set and the prerequisite
relations; tests cross-check the product planner against it.
"""

import datetime
from collections import deque


def _add_months(month_key, n):
    total = int(month_key[:4]) * 12 + (int(month_key[5:7]) - 1) + n
    return "%04d-%02d" % (total // 12, total % 12 + 1)


def _normalize(graph_def):
    nodes = {}
    for name, spec in graph_def["nodes"].items():
        spec = spec or {}
        inputs = []
        for inp in spec.get("inputs", []):
            inputs.append({
                "node": inp["node"],
                "window": inp.get("window", 1),
                "global": bool(inp.get("global", False)),
            })
        nodes[name] = {"output": spec.get("output", "day"), "inputs": inputs}
    return nodes


def reference_impact(graph_def, seeds):
    """seeds: iterable of (node, partition).

    Returns {"dirty": sorted ["node/partition"], "edges": sorted [(up, down)]}
    where edges are the dirty->dirty prerequisite relations implied by the
    declared input ranges.
    """
    nodes = _normalize(graph_def)
    consumers = {n: [] for n in nodes}
    for v, spec in nodes.items():
        for inp in spec["inputs"]:
            consumers[inp["node"]].append((v, inp))

    dirty = set(seeds)
    edges = set()
    queue = deque(dirty)
    while queue:
        u, up = queue.popleft()
        for v, inp in consumers.get(u, ()):
            if inp["global"]:
                targets = [up[:7]]
            elif nodes[v]["output"] == "day":
                base = datetime.date.fromisoformat(up)
                targets = [(base + datetime.timedelta(days=i)).isoformat()
                           for i in range(inp["window"])]
            else:
                targets = [_add_months(up, i) for i in range(inp["window"])]
            for vp in targets:
                edges.add((u + "/" + up, v + "/" + vp))
                if (v, vp) not in dirty:
                    dirty.add((v, vp))
                    queue.append((v, vp))
    return {
        "dirty": sorted(n + "/" + p for n, p in dirty),
        "edges": sorted(edges),
    }
