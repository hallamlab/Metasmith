from __future__ import annotations
import numpy as np
from typing import Any
from dataclasses import dataclass
from scipy.cluster.hierarchy import linkage, dendrogram
from scipy.spatial.distance import squareform

@dataclass
class LabelGroup[T]:
    group: list[T]
    groupi: list[int]
    index: int
    def GetName(self):
        a = self.group[0]
        n = f"{a}+{len(self.group)-1}" if len(self.group)>1 else f"{a}"
        return n
    def GetAny(self):
        return self.group[0]
    
def Deduplicate(z: np.ndarray, labels: list):
    row_groups = {}
    for i, row in enumerate(z):
        label = labels[i]
        k = tuple(row)
        row_groups[k] = row_groups.get(k, [])+[(label, i)]
    unique_rows = [g[0][1] for g in row_groups.values()]
    z = z[unique_rows]
    ulabels = [LabelGroup(index=i, group=[l for l, i in g], groupi=[i for l, i in g]) for i, g in enumerate(row_groups.values())]
    return z, ulabels

@dataclass
class DendrogramNode[T]:
    x: float
    y: float
    i: int
    name: T|None=None
    left: DendrogramNode[T]|None=None
    right: DendrogramNode[T]|None=None
    def IsLeaf(self):
        return self.left is None and self.right is None
    
    def Children(self):
        for c in [self.left, self.right]:
            if c is not None:
                yield c

    def Traverse(self, order="in"):
        if self.IsLeaf():
            yield self
            return
        
        left, right = self.Children()
        todo = {
            "pre": [self, left, right],
            "in": [left, self, right],
            "post": [left, right, self]
        }[order]
        for x in todo:
            if x == self:
                yield self
            else:
                yield from x.Traverse(order)
    
@dataclass
class LinkageResult[T]:
    labels: list[T]
    order: list[int]
    mat: np.ndarray
    tree: DendrogramNode
    root_distance: float
    linkage: Any

def HierarchicalCluster(Z: np.ndarray, labels: list|None = None, method="ward", metric="euclidean", distance_sort=False, count_sort=False, sort_order=None) -> LinkageResult:
    """
    method: [single, complete, average, weighted, centroid, median, ward]
    https://docs.scipy.org/doc/scipy/reference/generated/scipy.cluster.hierarchy.linkage.html
    
    metric: https://docs.scipy.org/doc/scipy/reference/generated/scipy.spatial.distance.pdist.html#scipy.spatial.distance.pdist 
    """
    class FloatDict[U]:
        def __init__(self, d: dict[float, U] = dict()):
            self.d = d
            self.index = sorted(list(d.keys()))

        def _binary_search(self, k: float):
            # binary search for the nearest key to account for floating point errors
            l, r = 0, len(self.index)-1
            while l < r:
                m = (l + r) // 2
                if self.index[m] < k:
                    l = m + 1
                else:
                    r = m
            return self.index[l]

        def Nearest(self, k: float):
            if k in self.d: return self.d[k]
            return self.d[self._binary_search(k)]

    @dataclass
    class _DendNode:
        x: float
        y: float
        i: int
        xs: tuple[float, float, float, float]
        ys: tuple[float, float, float, float]
        left: _DendNode|None
        right: _DendNode|None

        def __hash__(self) -> int:
            return self.i
        def IsLeaf(self):
            return self.left is None and self.right is None
        def LeftCoords(self):
            (x1, x2, x3, x4), (y1, y2, y3, y4) = self.xs, self.ys
            return x1, y1
        def RightCoords(self):
            (x1, x2, x3, x4), (y1, y2, y3, y4) = self.xs, self.ys
            return x4, y4
        def IsAt(self, x, y):
            if x == self.x and y == self.y: return True
            zero = 1.0e-4
            return abs(self.x - x) < zero and abs(self.y - y) < zero

    # ###############################################################
    # use scipy to do the clustering
    labels = list(labels) if labels is not None else list(range(Z.shape[0]))
    _Z = Z
    if metric=="precomputed": _Z = squareform(Z)
    linkage_data = linkage(_Z, method=method, metric=metric, optimal_ordering=False)
    _p: Any = dict(count_sort=count_sort, distance_sort=distance_sort) # to bypass type warning
    dend = dendrogram(linkage_data, no_plot=True, labels=list(range(len(labels))), **_p)

    # ###############################################################
    # convert the arrays of inscrutible numbers and unhelpful tree
    # tree structure from scipy into something useful 
    link_ys = dend["dcoord"]
    link_xs = dend["icoord"]
    clust_orderi = dend["ivl"]
    clust_order = [labels[i] for i in clust_orderi]

    _nodes_by_x = {}
    _nodes_by_y = {}
    nodes_by_i: dict[int, _DendNode] = {}
    nodes: list[_DendNode] = []
    for i, (xs, ys) in enumerate(zip(link_xs, link_ys)):
        (x1, x2, x3, x4), (y1, y2, y3, y4) = xs, ys
        nx = (x2+x3)/2
        ny = y2
        node = _DendNode(nx, ny, i, xs, ys, None, None)
        _nodes_by_x[nx] = _nodes_by_x.get(nx, []) + [i]
        _nodes_by_y[ny] = _nodes_by_y.get(ny, []) + [i]
        nodes_by_i[i] = node
        nodes.append(node)
    nodes_by_x: FloatDict[list] = FloatDict(_nodes_by_x)
    nodes_by_y: FloatDict[list] = FloatDict(_nodes_by_y)

    def get_node_at(x, y):
        candidates = set(nodes_by_x.Nearest(x) + nodes_by_y.Nearest(y))
        candidates = [i for i in candidates if nodes_by_i[i].IsAt(x, y)]
        if len(candidates) == 0: return
        assert len(candidates) == 1
        return nodes_by_i[candidates[0]]

    node_parents: dict[_DendNode, _DendNode] = {}
    for n in nodes:
        for k, (x, y) in [("left", n.LeftCoords()), ("right", n.RightCoords())]:
            child = get_node_at(x, y)
            if child is None: continue
            setattr(n, k, child)
            node_parents[child] = n

    _root = None
    for n in nodes:
        if n in node_parents: continue
        assert _root is None
        _root = n
    assert _root is not None

    root_dist = _root.y
    root = DendrogramNode(_root.x, _root.y, _root.i)
    todo: list[DendrogramNode] = [root]
    label_index = 0
    # seen = set()
    while len(todo) > 0:
        new = todo.pop()
        # if new.i in seen: continue
        # seen.add(new.i)
        if new.i == -1:
            new.i = clust_orderi[label_index]
            new.name = clust_order[label_index]
            label_index += 1
            continue
        _node = nodes_by_i[new.i]

        _ch_coords = [_node.LeftCoords(), _node.RightCoords()]
        _chl, _chr = sorted(_ch_coords, key=lambda x: x[0], reverse=True) # by x        
        for ch, (chx, chy) in [("right", _chl), ("left", _chr)]:
            _child = getattr(_node, ch)
            if _child is None:
                new_child = DendrogramNode(chx, chy, -1)
            else:
                new_child = DendrogramNode(chx, chy, _child.i)
            setattr(new, ch, new_child)
            todo.append(new_child)
    
    # ###############################################################
    # sync matrix order to clustering

    Z = Z[clust_orderi]
    if metric=="precomputed": Z = Z.T[clust_orderi].T

    # ###############################################################
    # order, if given

    if sort_order is not None:
        _pos = {}
        def update_pos(n: DendrogramNode):
            if n.i not in _pos:
                if n.IsLeaf():
                    _positions = [sort_order[n.i]]
                    _pos[n.i] = _positions
                else:
                    assert n.left is not None
                    assert n.right is not None
                    _positions = update_pos(n.left) + update_pos(n.right)
                    _pos[n.i] = _positions
                    if n.left.x > n.right.x:
                        n.left, n.right = n.right, n.left
            _positions = _pos[n.i]
            n.x = sum(_positions)/len(_positions)
            return _pos[n.i]
        update_pos(root)

    # ###############################################################
    # normalize positions

    max_x, min_x, = -np.inf, np.inf
    max_y, min_y, = -np.inf, np.inf
    for node in root.Traverse():
        min_x, max_x = min(min_x, node.x), max(max_x, node.x)
        min_y, max_y = min(min_y, node.y), max(max_y, node.y)
    for node in root.Traverse():
        node.x = float((node.x - min_x) / (max_x - min_x))
        node.y = float((node.y - min_y) / (max_y - min_y))

    return LinkageResult(clust_order, clust_orderi, Z[clust_orderi], root, root_dist, linkage_data)
