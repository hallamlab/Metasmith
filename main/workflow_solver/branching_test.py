from metasmith.models.solver import solve_by_mcts
from metasmith.models.solver import Transform, Endpoint, Solution
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

    have = [
        Endpoint(properties={"given"}),
    ]

    target = Transform()
    target.AddRequirement(properties={"given"})
    sol = solve_by_mcts(given=have, target=target, transforms=transforms)
    print(sol.complete, sol._iterations)
    assert sol.complete

    # for i, plan in enumerate(sol.dependency_plans):
    #     print(f">>> {i+1}")
    #     for appl in plan:
    #         print(f"    {appl.transform}")
    #         for d, e in appl.used.items():
    #             print(f"        {e}")

def simple():
    transforms = []
    t = Transform() # assembly <-> bins
    t.AddRequirement(properties={"assembly"})
    t.AddProduct(properties={"bins"})
    transforms.append(t)

    t = Transform() # bins <-> tax
    t.AddRequirement(properties={"bins"})
    t.AddProduct(properties={"tax"})
    transforms.append(t)

    have = [
        Endpoint(properties={"assembly"}),
    ]

    target = Transform()
    a = target.AddRequirement(properties={"bins"})
    b = target.AddRequirement(properties={"tax"}, parents={a})
    sol = solve_by_mcts(given=have, target=target, transforms=transforms)
    print(sol.complete, sol._iterations)
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

    given = [Endpoint(properties={"start"})]
    target = Transform()
    target.AddRequirement(properties={"target"})
    sol = solve_by_mcts(
        given=given,
        target=target,
        transforms=transforms,
    )
    print(sol.complete, len(sol.dependency_plan), sol._iterations)
    assert sol.complete
    # for i, states in enumerate(sol._history):
    #     print(f">>> {i} | states: {len(states)}")
    #     for j, state in enumerate(states):
    #         print(f"  {j} | steps: {len(state.steps)}")
    #         for appl in state.steps:
    #             print(f"    {appl.transform}")
    #             for d, e in appl.used.items():
    #                 print(f"      {d} {e}")
    #                 # print(f"        ---")
    #                 for pgroup in appl.produced:
    #                     print(f"      .")
    #                     for d, e in pgroup.items():
    #                         print(f"      {d} {e}")
    #     print()


    # for i, states in enumerate(sol._refiner_histories):
    #     print(f">>> {i} | iterations: {len(states)}")
    #     for j, state in enumerate(states):
    #         print(f"  {j} | valid: {state.valid}")
    #         for s in state.steps:
    #             print(f"        {s.transform}")
    #     print()

    for appl in sol.dependency_plan:
        print(f"    {appl.transform}")
        for d, e in appl.used.items():
            print(f"        {d} {e}")
        # print(f"        ---")
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

    given = [Endpoint(properties={"start"})]
    target = Transform()
    target.AddRequirement(properties={"target"})
    sol = solve_by_mcts(
        given=given,
        target=target,
        transforms=transforms,
    )
    print(sol.complete, len(sol.dependency_plan), sol._iterations)
    sol.RenderDAG("./cache/br2")
    assert sol.complete
    # for i, states in enumerate(sol._history):
    #     print(f">>> {i} | states: {len(states)}")
    #     for j, state in enumerate(states):
    #         print(f"  {j} | steps: {len(state.steps)}")
    #         for appl in state.steps:
    #             print(f"    {appl.transform}")
    #             for d, e in appl.used.items():
    #                 print(f"      {d} {e}")
    #                 # print(f"        ---")
    #                 for pgroup in appl.produced:
    #                     print(f"      .")
    #                     for d, e in pgroup.items():
    #                         print(f"      {d} {e}")
    #     print()


    # for i, states in enumerate(sol._refiner_histories):
    #     print(f">>> {i} | iterations: {len(states)}")
    #     for j, state in enumerate(states):
    #         print(f"  {j} | valid: {state.valid}")
    #         for s in state.steps:
    #             print(f"        {s.transform}")
    #     print()

    # for appl in sol.dependency_plan:
    #     print(f"    [{appl.initial_timeline}] {appl.transform}")
    #     for d, e in appl.used.items():
    #         print(f"        {d} {e}")
    #     # print(f"        ---")
    #     for pgroup in appl.produced:
    #         print(f"        .")
    #         for d, e in pgroup.items():
    #             print(f"        {d} {e}")

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

    # used by both branches, identical signatures,
    # but can't merge due to lineage
    tr = Transform()
    tr.AddRequirement(properties={"z"})
    tr.AddProduct(properties={"z2"})
    transforms.append(tr)

    # lineage constraint contained within branch
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

    # used by both branches, identical signatures,
    # but CAN merge despite to lineage
    tr = Transform()
    dep = tr.AddRequirement(properties={"y"})
    tr.AddRequirement(properties={"y2"}, parents={dep})
    tr.AddProduct(properties={"target"})
    transforms.append(tr)

    given = [Endpoint(properties={"start"})]
    target = Transform()
    target.AddRequirement(properties={"target"})
    sol = solve_by_mcts(
        given=given,
        target=target,
        transforms=transforms,
    )
    print(sol.complete, len(sol.dependency_plan), sol._iterations)
    sol.RenderDAG("./cache/br3", format="png")
    assert sol.complete
    # for i, states in enumerate(sol._history):
    #     print(f">>> {i} | states: {len(states)}")
    #     for j, state in enumerate(states):
    #         print(f"  {j} | steps: {len(state.steps)}")
    #         for appl in state.steps:
    #             print(f"    {appl.transform}")
    #             for d, e in appl.used.items():
    #                 print(f"      {d} {e}")
    #                 # print(f"        ---")
    #                 for pgroup in appl.produced:
    #                     print(f"      .")
    #                     for d, e in pgroup.items():
    #                         print(f"      {d} {e}")
    #     print()


    # for i, states in enumerate(sol._refiner_histories):
    #     print(f">>> {i} | iterations: {len(states)}")
    #     for j, state in enumerate(states):
    #         print(f"  {j} | valid: {state.valid}")
    #         for s in state.steps:
    #             print(f"        {s.transform}")
    #     print()

    # for appl in sol.dependency_plan:
    #     print(f"    {appl.transform}")
    #     for d, e in appl.used.items():
    #         print(f"        {d} {e}")
    #     # print(f"        ---")
    #     for pgroup in appl.produced:
    #         print(f"        .")
    #         for d, e in pgroup.items():
    #             print(f"        {d} {e}")

# trivial()
# simple()
# branching_1()
# branching_2()
branching_3()

# test when branching is not needed
# add joining during mcts
#   when used appl in solved branch,
#   check if remaining transforms in branch can be used to FF to solution


# for s in plan.state[0].steps:
#     print(s.transform)


# t = Transform()
# t.AddRequirement(properties={"bins"})
# t.AddProduct(properties={"assembly"})
# transforms.append(t)

# t = Transform() # bins <-> tax
# t.AddRequirement(properties={"bins"})
# t.AddProduct(properties={"tax"})
# transforms.append(t)

# t = Transform()
# t.AddRequirement(properties={"tax"})
# t.AddProduct(properties={"bins"})
# transforms.append(t)

# t = Transform() # bins <-> contigs
# t.AddRequirement(properties={"bins"})
# t.AddProduct(properties={"contigs"})
# transforms.append(t)

# t = Transform()
# t.AddRequirement(properties={"contigs"})
# t.AddProduct(properties={"bins"})
# transforms.append(t)

# t = Transform() # contigs <-> ORFs
# t.AddRequirement(properties={"contigs"})
# t.AddProduct(properties={"ORFs"})
# transforms.append(t)

# t = Transform()
# t.AddRequirement(properties={"ORFs"})
# t.AddProduct(properties={"contigs"})
# transforms.append(t)

# t = Transform() # ORFs <-> annotation
# t.AddRequirement(properties={"ORFs"})
# t.AddProduct(properties={"annotation"})
# transforms.append(t)
# t = Transform() 
# t.AddRequirement(properties={"annotation"})
# t.AddProduct(properties={"ORFs"})
# transforms.append(t)
