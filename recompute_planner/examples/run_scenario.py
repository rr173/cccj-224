"""End-to-end demo of the hand-computed scenario.

Graph:  A -> B -> C -> M(global month), A -> W(7-day window), X1 -> X2 (unrelated)
Late events: A/2026-11-05 and A/2026-10-31.
"""

import os
import sys
import tempfile

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from planner import RecomputePlanner  # noqa: E402
from tests.test_planner import build_graph, seed_all  # noqa: E402


def main():
    db = os.path.join(tempfile.mkdtemp(), "demo.db")
    with RecomputePlanner(db) as p:
        gid = p.compile_graph(build_graph())
        seed_all(p)
        print(f"graph compiled: {gid}")

        print(p.ingest_event("e1", "A", "2026-11-05", 1, "d1"))
        print(p.ingest_event("e2", "A", "2026-10-31", 2, "d2"))
        print("duplicate re-delivery:", p.ingest_event("e1", "A", "2026-11-05", 3, "d1"))

        pid = p.create_plan()
        print(f"\nplan {pid} (binds graph+events+published state)")
        print(f"{'#':>3} {'item':<18} {'ver':<4} waits_on")
        for it in p.list_plan_items(pid, limit=100)["items"]:
            waits = ", ".join(it["waits_on"]) or "-"
            print(f"{it['publish_order']:>3} {it['node'] + '/' + it['partition']:<18} "
                  f"{it['new_version']:<4} {waits}")

        w = next(i for i in p.list_plan_items(pid, node="W", limit=100)["items"]
                 if i["partition"] == "2026-11-05")
        print("\nevidence W/2026-11-05 input versions:", w["input_versions"]["A"])
        print("reasons:", *w["reasons"], sep="\n  ")

        skip = p.get_skippable(pid, node="X1", limit=3)
        print(f"\nskippable sample (X branch): {[s['node'] + '/' + s['partition'] for s in skip['items']]}")

        # simulate execution: B fails, successors stay unpublished (blocked)
        for it in p.list_plan_items(pid, limit=100)["items"]:
            outcome = "failed" if (it["node"], it["partition"]) == ("B", "2026-11-05") else "success"
            try:
                p.advance(pid, it["node"], it["partition"], outcome)
            except Exception as exc:
                print(f"  {it['node']}/{it['partition']}: refused -> {exc}")
        print("\nplan status:", p.get_plan(pid)["status"])
        print("C/2026-11-05 current version (stayed old):",
              p.current_version("C", "2026-11-05"))
        print("A/2026-11-05 history (old version retained):",
              [h["version"] for h in p.version_history("A", "2026-11-05")])

        pid2 = p.create_plan()  # carry-over debt only
        print("\nretry plan items:",
              [f"{i['node']}/{i['partition']}" for i in p.list_plan_items(pid2, limit=100)["items"]])
        for it in p.list_plan_items(pid2, limit=100)["items"]:
            p.advance(pid2, it["node"], it["partition"], "success")

        print("replay plan1:", p.replay_plan(pid)["match"],
              "| replay plan2:", p.replay_plan(pid2)["match"])
        print("stats:", p.stats())


if __name__ == "__main__":
    main()
