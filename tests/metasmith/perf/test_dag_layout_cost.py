import random
import time

from metasmith.models.dag_layout import layout


def _sparse_dag(seed, n):
    rng = random.Random(seed)
    names = [f"n{i}" for i in range(n)]
    edges = []
    for j in range(1, n):
        for _ in range(1 + (rng.random() < 0.8)):
            edges.append((names[rng.randrange(max(0, j - 12), j)], names[j]))
    return names, edges


def test_a_hundred_nodes_solve_inside_a_second():
    names, edges = _sparse_dag(100, 100)
    # the calling thread only: numpy's BLAS workers spin while idle, and
    # process_time counted that spin as up to 0.8s of layout
    t = time.thread_time()
    layout(names, edges)
    assert time.thread_time() - t <= 1.0
