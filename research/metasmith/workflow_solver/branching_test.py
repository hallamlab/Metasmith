from metasmith.models.solver import solve_by_mcts
from metasmith.models.solver import Transform, Endpoint, Solution, Application
from metasmith.models.workflow import WorkflowPlan

def trivial():
    transforms = []
    t = Transform()
    t.AddRequirement(properties={"x"})
    t.AddProduct(properties={"y"})
    transforms.append(t)

    t = Transform()
    t.AddRequirement(properties={"a"})
    t.AddProduct(properties={"b"})
    transforms.append(t)

    have = {
        Endpoint(properties={"given"})
    }

    target = Transform()
    target.AddRequirement(properties={"given"})
    sol = solve_by_mcts(given=[have], target=target, transforms=transforms)
    print(sol.complete, sol._iterations)
    assert sol.complete


def simple():
    transforms = []
    t = Transform()
    t.AddRequirement(properties={"assembly"})
    t.AddProduct(properties={"bins"})
    transforms.append(t)

    t = Transform()
    t.AddRequirement(properties={"bins"})
    t.AddProduct(properties={"tax"})
    transforms.append(t)

    have = {
        Endpoint(properties={"assembly"}),
    }

    target = Transform()
    a = target.AddRequirement(properties={"bins"})
    b = target.AddRequirement(properties={"tax"}, parents={a})
    sol = solve_by_mcts(given=[have], target=target, transforms=transforms)
    print(sol.complete, sol._iterations)
    assert sol.complete

def simple_2():
    transforms = []
    t = Transform()
    t.AddRequirement(properties={"a"})
    t.AddProduct(properties={"x"})
    transforms.append(t)

    t = Transform()
    t.AddRequirement(properties={"b"})
    t.AddProduct(properties={"x"})
    transforms.append(t)

    target = Transform()
    a = target.AddRequirement(properties={"x"})
    sol = solve_by_mcts(given=[
        {
            Endpoint(properties={"a"}),
        },
        {
            Endpoint(properties={"b"}),
        },
    ], target=target, transforms=transforms)
    print(sol.complete, sol._iterations)
    sol.RenderDAG("./cache/s2")
    assert sol.complete

def loop_1():
    transforms = []
    t = Transform()
    t.AddRequirement(properties={"start"})
    t.AddProduct(properties={"a"})
    transforms.append(t)

    t = Transform()
    t.AddRequirement(properties={"a"})
    t.AddProduct(properties={"b"})
    transforms.append(t)

    t = Transform()
    t.AddRequirement(properties={"b"})
    t.AddProduct(properties={"a"})
    transforms.append(t)

    t = Transform()
    t.AddRequirement(properties={"b"})
    t.AddProduct(properties={"c"})
    transforms.append(t)

    t = Transform()
    t.AddRequirement(properties={"c"})
    t.AddProduct(properties={"b"})
    transforms.append(t)

    t = Transform()
    t.AddRequirement(properties={"c"})
    t.AddProduct(properties={"target"})
    transforms.append(t)

    target = Transform()
    a = target.AddRequirement(properties={"a"})
    target.AddRequirement(properties={"target"}, parents={a})
    sol = solve_by_mcts(given=[
        {
            Endpoint(properties={"c"}),
        },
    ], target=target, transforms=transforms)
    print(sol.complete, sol._iterations)
    sol.RenderDAG("./cache/l1", keys=False)
    assert sol.complete

def branching_1():
    transforms = []

    tr = Transform()
    tr.AddRequirement(properties={"start"})
    tr.AddProduct(properties={"x"})
    transforms.append(tr)

    tr = Transform()
    tr.AddRequirement(properties={"x"})
    tr.AddProduct(properties={"a"})
    tr.NewProductGroup()
    tr.AddProduct(properties={"b"})
    transforms.append(tr)

    tr = Transform()
    tr.AddRequirement(properties={"a"})
    tr.AddProduct(properties={"a2"})
    transforms.append(tr)

    tr = Transform()
    tr.AddRequirement(properties={"a2"})
    tr.AddProduct(properties={"y", "a"})
    transforms.append(tr)

    tr = Transform()
    tr.AddRequirement(properties={"b"})
    tr.AddProduct(properties={"b2"})
    transforms.append(tr)

    tr = Transform()
    tr.AddRequirement(properties={"b2"})
    tr.AddProduct(properties={"y", "b"})
    transforms.append(tr)

    tr = Transform()
    tr.AddRequirement(properties={"y"})
    tr.AddProduct(properties={"target"})
    transforms.append(tr)

    given = {Endpoint(properties={"start"})}
    target = Transform()
    target.AddRequirement(properties={"target"})
    sol = solve_by_mcts(
        given=[given],
        target=target,
        transforms=transforms,
    )
    print(sol.complete, len(sol.dependency_plan), sol._iterations)
    assert sol.complete


    for appl in sol.dependency_plan:
        print(f"    {appl.transform}")
        for d, e in appl.used.items():
            print(f"        {d} {e}")
        for pgroup in appl.produced:
            print(f"        .")
            for d, e in pgroup.items():
                print(f"        {d} {e}")

def branching_2():
    transforms = []

    tr = Transform()
    tr.AddRequirement(properties={"start"})
    tr.AddProduct(properties={"x"})
    transforms.append(tr)

    tr = Transform()
    tr.AddRequirement(properties={"x"})
    tr.AddProduct(properties={"a"})
    tr.NewProductGroup()
    tr.AddProduct(properties={"b"})
    transforms.append(tr)

    tr = Transform()
    tr.AddRequirement(properties={"a"})
    tr.AddProduct(properties={"a2"})
    transforms.append(tr)

    tr = Transform()
    tr.AddRequirement(properties={"a2"})
    tr.AddProduct(properties={"y", "a"})
    transforms.append(tr)

    tr = Transform()
    tr.AddRequirement(properties={"b"})
    tr.AddProduct(properties={"b2"})
    transforms.append(tr)

    tr = Transform()
    tr.AddRequirement(properties={"b"})
    tr.AddProduct(properties={"f"})
    tr.NewProductGroup()
    tr.AddProduct(properties={"g"})
    transforms.append(tr)

    tr = Transform()
    tr.AddRequirement(properties={"f"})
    tr.AddProduct(properties={"y", "yf"})
    transforms.append(tr)

    tr = Transform()
    tr.AddRequirement(properties={"g"})
    tr.AddProduct(properties={"g2"})
    transforms.append(tr)

    tr = Transform()
    tr.AddRequirement(properties={"g2"})
    tr.AddProduct(properties={"y", "g"})
    transforms.append(tr)

    tr = Transform()
    tr.AddRequirement(properties={"y"})
    tr.AddProduct(properties={"target"})
    transforms.append(tr)

    given = {Endpoint(properties={"start"})}
    target = Transform()
    target.AddRequirement(properties={"target"})
    sol = solve_by_mcts(
        given=[given],
        target=target,
        transforms=transforms,
    )
    print(sol.complete, len(sol.dependency_plan), sol._iterations)
    sol.RenderDAG("./cache/br2")
    assert sol.complete


def branching_3():
    transforms = []

    tr = Transform()
    tr.AddRequirement(properties={"start"})
    tr.AddProduct(properties={"x"})
    transforms.append(tr)

    tr = Transform()
    tr.AddRequirement(properties={"x"})
    tr.AddProduct(properties={"a", "v"})
    tr.NewProductGroup()
    tr.AddProduct(properties={"b", "v"})
    transforms.append(tr)

    tr = Transform()
    tr.AddRequirement(properties={"v"})
    tr.AddProduct(properties={"z"})
    transforms.append(tr)

    tr = Transform()
    tr.AddRequirement(properties={"z"})
    tr.AddProduct(properties={"z2"})
    transforms.append(tr)

    tr = Transform()
    dep = tr.AddRequirement(properties={"z"})
    tr.AddRequirement(properties={"z2"}, parents={dep})
    tr.AddProduct(properties={"z3"})
    transforms.append(tr)

    tr = Transform()
    dep = tr.AddRequirement(properties={"b"})
    tr.AddRequirement(properties={"z3"}, parents={dep})
    tr.AddProduct(properties={"y"})
    transforms.append(tr)

    tr = Transform()
    dep = tr.AddRequirement(properties={"a"})
    tr.AddRequirement(properties={"z3"}, parents={dep})
    tr.AddProduct(properties={"y"})
    transforms.append(tr)

    tr = Transform()
    tr.AddRequirement(properties={"y"})
    tr.AddProduct(properties={"y2"})
    transforms.append(tr)

    tr = Transform()
    dep = tr.AddRequirement(properties={"y"})
    tr.AddRequirement(properties={"y2"}, parents={dep})
    tr.AddProduct(properties={"target"})
    transforms.append(tr)

    given = {Endpoint(properties={"start"})}
    target = Transform()
    target.AddRequirement(properties={"target"})
    sol = solve_by_mcts(
        given=[given],
        target=target,
        transforms=transforms,
    )
    print(sol.complete, len(sol.dependency_plan), sol._iterations)
    sol.RenderDAG("./cache/br3", format="svg")
    assert sol.complete


def branching_4():
    transforms = []

    tr = Transform()
    tr.AddRequirement(properties={"start"})
    tr.AddProduct(properties={"x", "a"})
    tr.NewProductGroup()
    tr.AddProduct(properties={"x", "b"})
    transforms.append(tr)

    tr = Transform()
    tr.AddRequirement(properties={"a"})
    tr.AddProduct(properties={"b"})
    transforms.append(tr)

    tr = Transform()
    tr.AddRequirement(properties={"b"})
    tr.AddProduct(properties={"target"})
    transforms.append(tr)

    estart = Endpoint(properties={"start"})
    target = Transform()
    target.AddRequirement(properties={"b"})
    sol = solve_by_mcts(
        given=[
            {Endpoint(properties={"a"})},
            {Endpoint(properties={"b"})},
        ],
        target=target,
        transforms=transforms,
    )
    print(sol.complete, len(sol.dependency_plan), sol._iterations)
    sol.RenderDAG("./cache/br4", format="svg", keys=False)
    assert sol.complete

def branching_5():
    transforms = []

    tr = Transform()
    tr.AddRequirement(properties={"start"})
    tr.AddProduct(properties={"x", "a"})
    tr.NewProductGroup()
    tr.AddProduct(properties={"x", "a", "b"})
    transforms.append(tr)
    tr.NewProductGroup()
    tr.AddProduct(properties={"x", "b"})
    transforms.append(tr)

    tr = Transform()
    tr.AddRequirement(properties={"a"})
    tr.AddProduct(properties={"target"})
    transforms.append(tr)

    tr = Transform()
    tr.AddRequirement(properties={"b"})
    tr.AddProduct(properties={"target"})
    transforms.append(tr)

    estart = Endpoint(properties={"start"})
    target = Transform()
    target.AddRequirement(properties={"target"})
    sol = solve_by_mcts(
        given=[
            {Endpoint(properties={"start"})},
        ],
        target=target,
        transforms=transforms,
    )
    print(sol.complete, len(sol.dependency_plan), sol._iterations)
    sol.RenderDAG("./cache/br5", format="svg", keys=False)
    assert sol.complete

def branching_6():
    transforms = []

    tr = Transform()
    tr.AddRequirement(properties={"start"})
    tr.AddProduct(properties={"read_meta"})
    transforms.append(tr)

    tr = Transform()
    tr.AddRequirement(properties={"read_meta"})
    tr.AddProduct(properties={"sra"})
    transforms.append(tr)

    tr = Transform()
    x = tr.AddRequirement(properties={"read_meta"})
    tr.AddRequirement(properties={"sra"}, parents={x})
    tr.AddProduct(properties={"reads", "long", "single"})
    tr.NewProductGroup()
    tr.AddProduct(properties={"reads", "short", "single"})
    tr.NewProductGroup()
    tr.AddProduct(properties={"reads", "short", "paired"})
    transforms.append(tr)

    tr = Transform()
    tr.AddRequirement(properties={"reads", "short"})
    tr.AddProduct(properties={"read_qc"})
    transforms.append(tr)

    tr = Transform()
    tr.AddRequirement(properties={"reads", "long"})
    tr.AddProduct(properties={"read_qc"})
    transforms.append(tr)

    tr = Transform()
    tr.AddRequirement(properties={"reads", "long"})
    tr.AddProduct(properties={"clean_reads", "long"})
    transforms.append(tr)

    tr = Transform()
    meta = tr.AddRequirement(properties={"read_meta"})
    reads = tr.AddRequirement(properties={"reads", "short"}, parents={meta})
    tr.AddRequirement(properties={"read_qc"}, parents={reads})
    tr.AddProduct(properties={"clean_reads", "short"})
    transforms.append(tr)

    tr = Transform()
    x = tr.AddRequirement(properties={"reads", "long"})
    tr.AddRequirement(properties={"clean_reads", "long"}, parents={x})
    tr.AddRequirement(properties={"read_qc"}, parents={x})
    tr.AddProduct(properties={"assembly"})
    transforms.append(tr)

    tr = Transform()
    meta = tr.AddRequirement(properties={"read_meta"})
    tr.AddRequirement(properties={"clean_reads", "short"}, parents={meta})
    tr.AddProduct(properties={"assembly"})
    transforms.append(tr)

    tr = Transform()
    meta = tr.AddRequirement(properties={"read_meta"})
    tr.AddRequirement(properties={"assembly"}, parents={meta})
    tr.AddProduct(properties={"target"})
    transforms.append(tr)

    tr = Transform()
    meta = tr.AddRequirement(properties={"read_meta"})
    tr.AddRequirement(properties={"clean_reads"}, parents={meta})
    tr.AddRequirement(properties={"read_qc"}, parents={meta})
    tr.AddRequirement(properties={"assembly"}, parents={meta})
    tr.AddProduct(properties={"assembly_stats"})
    transforms.append(tr)

    target = Transform()
    target.AddRequirement(properties={"assembly_stats"})
    sol = solve_by_mcts(
        given=[
            {Endpoint(properties={"start"})},
        ],
        target=target,
        transforms=transforms,
    )
    print(sol.complete, len(sol.dependency_plan), sol._iterations)
    sol.RenderDAG("./cache/br6", format="svg", keys=False)
    assert sol.complete


branching_6()
