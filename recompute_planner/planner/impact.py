"""Pure impact computation: dirty-set propagation and plan-item construction.

These functions are deterministic and side-effect free; both the live planner
and plan replay call them, which is what makes replay-by-plan-id verifiable.
"""

from collections import deque

from .dates import add_period, days_of_month, month_of
from .graph import consumers, levels


def compute_impact(gdef, seeds):
    """Propagate dirty seeds along declared input ranges.

    seeds: {(node, partition): [reason, ...]}
    returns: {(node, partition): [reason, ...]} -- the minimal dirty set.
    """
    adj = consumers(gdef)
    dirty = {k: list(v) for k, v in seeds.items()}
    queue = deque(seeds.keys())
    while queue:
        u, up = queue.popleft()
        for v, spec in adj.get(u, ()):
            gran = gdef["nodes"][v]["output"]
            for vp, detail in _expand(spec, gran, up):
                reason = f"reads dirty {u}/{up}: {detail}"
                key = (v, vp)
                if key not in dirty:
                    dirty[key] = [reason]
                    queue.append(key)
                elif reason not in dirty[key]:
                    dirty[key].append(reason)
    return dirty


def _expand(spec, out_gran, up):
    """Consumer partitions that read upstream partition ``up``."""
    if spec["global"]:
        m = month_of(up)
        yield m, f"global month {m} contains day {up}"
        return
    w = spec["window"]
    for i in range(w):
        p = add_period(up, i, out_gran)
        if w == 1:
            detail = "same-partition input"
        else:
            start = add_period(p, -(w - 1), out_gran)
            detail = f"declared window [{start}..{p}] includes {up}"
        yield p, detail


def input_partitions(spec, out_gran, pkey):
    """Upstream partitions read by consumer partition ``pkey`` (ascending)."""
    if spec["global"]:
        return list(days_of_month(pkey))
    w = spec["window"]
    return [add_period(pkey, -(w - 1) + i, out_gran) for i in range(w)]


def next_version(current):
    return "v1" if current is None else f"v{int(current[1:]) + 1}"


def build_plan_items(gdef, seeds, published_versions):
    """Build the ordered plan items for a dirty seed set.

    published_versions: {"node|partition": "vN"} of currently published versions.
    Returns a list of item dicts with publish_order, new_version, reasons,
    input_versions (dependency evidence) and waits_on (in-plan prerequisites).
    """
    lev = levels(gdef)
    dirty = compute_impact(gdef, seeds)
    keys = sorted(dirty, key=lambda k: (lev[k[0]], k[1], k[0]))
    assigned = {}
    for node, pkey in keys:
        assigned[(node, pkey)] = next_version(published_versions.get(f"{node}|{pkey}"))
    items = []
    for order, (node, pkey) in enumerate(keys, 1):
        spec = gdef["nodes"][node]
        inputs, waits = {}, []
        for inp in spec["inputs"]:
            u = inp["node"]
            vers = {}
            for up in input_partitions(inp, spec["output"], pkey):
                if (u, up) in assigned:
                    vers[up] = assigned[(u, up)]
                    waits.append((lev[u], up, u))
                else:
                    vers[up] = published_versions.get(f"{u}|{up}")
            inputs[u] = vers
        items.append({
            "node": node,
            "partition": pkey,
            "publish_order": order,
            "new_version": assigned[(node, pkey)],
            "reasons": list(dirty[(node, pkey)]),
            "input_versions": inputs,
            "waits_on": [f"{u}/{p}" for _, p, u in sorted(waits)],
        })
    return items
