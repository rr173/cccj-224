"""Dependency-graph compilation: normalization, validation, topology.

Graph definition format::

    {"nodes": {
        "A": {"output": "day", "inputs": []},
        "W": {"output": "day",
              "inputs": [{"node": "A", "window": 7, "cross_day": true}]},
        "M": {"output": "month",
              "inputs": [{"node": "C", "global": true}]},
    }}

Input spec fields (defaults in brackets): ``node`` (upstream, required),
``window`` (1), ``cross_day`` (false), ``global`` (false).
"""

import re

from .errors import GraphError

_NAME_RE = re.compile(r"^[A-Za-z0-9_.-]+$")


def normalize(gdef):
    """Fill defaults and validate; raise GraphError on any contradiction."""
    if not isinstance(gdef, dict) or not isinstance(gdef.get("nodes"), dict) or not gdef["nodes"]:
        raise GraphError("graph def must be {'nodes': {name: spec}} with at least one node")
    nodes = {}
    for name, spec in gdef["nodes"].items():
        if not _NAME_RE.match(name):
            raise GraphError(f"invalid node name {name!r}")
        spec = spec or {}
        output = spec.get("output", "day")
        if output not in ("day", "month"):
            raise GraphError(f"{name}: output must be 'day' or 'month', got {output!r}")
        inputs = []
        for inp in spec.get("inputs", []):
            if not isinstance(inp, dict) or "node" not in inp:
                raise GraphError(f"{name}: every input must name an upstream 'node'")
            inputs.append({
                "node": inp["node"],
                "window": inp.get("window", 1),
                "cross_day": bool(inp.get("cross_day", False)),
                "global": bool(inp.get("global", False)),
            })
        nodes[name] = {"output": output, "inputs": inputs}
    norm = {"nodes": nodes}
    _validate(norm)
    return norm


def _validate(g):
    nodes = g["nodes"]
    for v, spec in nodes.items():
        for inp in spec["inputs"]:
            u = inp["node"]
            if u not in nodes:
                raise GraphError(f"missing dependency: node {v!r} reads unknown node {u!r}")
            if u == v:
                raise GraphError(f"cycle: node {v!r} reads itself")
            w = inp["window"]
            if not isinstance(w, int) or isinstance(w, bool) or w < 1:
                raise GraphError(f"{v}: window must be an integer >= 1, got {w!r}")
            if inp["global"]:
                if spec["output"] != "month":
                    raise GraphError(
                        f"{v}: global aggregation requires month output, got {spec['output']!r}")
                if nodes[u]["output"] != "day":
                    raise GraphError(
                        f"{v}: global aggregation requires day-partitioned upstream {u!r}")
            else:
                if nodes[u]["output"] != spec["output"]:
                    raise GraphError(
                        f"{v}: granularity mismatch with upstream {u!r} "
                        f"({nodes[u]['output']} -> {spec['output']}); declare global=true for day->month")
                if w > 1 and not inp["cross_day"]:
                    raise GraphError(
                        f"{v}: declared window {w} contradicts cross_day=false")
    _check_acyclic(nodes)


def _check_acyclic(nodes):
    indeg = {n: 0 for n in nodes}
    for v, spec in nodes.items():
        indeg[v] = len(spec["inputs"])
    queue = [n for n, d in indeg.items() if d == 0]
    seen = 0
    while queue:
        x = queue.pop()
        seen += 1
        for v, spec in nodes.items():
            for inp in spec["inputs"]:
                if inp["node"] == x:
                    indeg[v] -= 1
                    if indeg[v] == 0:
                        queue.append(v)
    if seen != len(nodes):
        raise GraphError("dependency graph has a cycle")


def levels(g):
    """Topological level per node (sources = 0). Graph is validated acyclic."""
    lev = {}

    def visit(n):
        if n not in lev:
            ins = g["nodes"][n]["inputs"]
            lev[n] = 0 if not ins else 1 + max(visit(i["node"]) for i in ins)
        return lev[n]

    for name in g["nodes"]:
        visit(name)
    return lev


def consumers(g):
    """Adjacency: upstream node -> [(consumer node, input spec)]."""
    adj = {n: [] for n in g["nodes"]}
    for v, spec in g["nodes"].items():
        for inp in spec["inputs"]:
            adj[inp["node"]].append((v, inp))
    return adj
