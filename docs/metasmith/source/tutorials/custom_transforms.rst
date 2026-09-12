.. role:: python(code)
    :language: python

Custom transforms
############################################################

This tutorial will demonstrate the integration of an external software tool, :python:`fastANI`, into the Metasmith
framework.

The Jupyter notebook for this tutorial can be obtained by:

.. code-block:: bash
    :caption: Terminal

    $ msm get tutorials/custom_transforms.ipynb

The completed fastANI transform can be obtained by:

.. code-block:: bash
    :caption: Terminal

    $ msm get transforms/fastani.py

Prerequisites
============================================================

- `Metasmith is installed <../setup/install.html>`_ along with either Docker or Apptainer, since we will be deploying an agent locally
- `A tutorial workspace has been setup for Jupyter notebooks <../setup/tutorials.html>`_
- You have completed the tutorial: `My first agent <my_first_agent.html>`_ since this will be a direct continuation

.. note::

    If you are following along outside of Jupyter (for example as plain Python
    scripts), set :python:`WORKSPACE = Path("./")` and
    :python:`MLIB = WORKSPACE/"MetasmithLibraries"` at the top of each script,
    and treat any :python:`ipynbButtonLink(...)` calls as illustrative — they
    are notebook helpers and can be skipped.

The shape of this tutorial
============================================================

A transform earns its keep by *connecting* existing transforms — turning two
disconnected halves of the type graph into a single chain that the solver
can plan over.

Upstream of fastANI, the standard library already provides
:python:`getNcbiAssembly` (in the :python:`logistics` transform library),
which fetches genomes from NCBI and produces :python:`sequences::assembly`.

Downstream of fastANI, we would like to visualise the all-vs-all comparison
as a *pairwise* heatmap — one cell per genome pair, color-coded by ANI.
There is no transform for that today either, so we will add a tiny
companion stub called :python:`ani_heatmap` that consumes
:python:`ani::table` and produces :python:`pangenome::heatmap`.

Today, :python:`sequences::assembly` and :python:`pangenome::heatmap` are
disconnected: nothing turns a group of genomes into an ANI matrix.
By adding :python:`fastani` (assembly + pangenome → ani::table), both
connections light up at once: getNcbiAssembly can feed into fastani, and
fastani can feed into ani_heatmap. Below we will render each of these
two connections as its own DAG, and close with a short remark on chaining
all three transforms end to end.

Modelling a new transform
============================================================

Metasmith models each tool as a transform between data types. Each transform is described in terms of a contact consisting of
required inputs and promised outputs.
fastANI calculates the average nucleotide identity (ANI) between two nucleotide sequences, typically genomes.
The contract for fastANI should therefore include that it requires a list of genomes and produces ANI values.
The data type :python:`sequences::assembly` already exists for genomes, but we will need to create a new one for ANI.

To begin, we will create a :python:`TransformInstanceLibrary` and add a stub for fastANI.

.. code-block:: python
    :caption: Jupyter
    :linenos:

    ani_transforms_path = WORKSPACE/"ani_transforms"
    ani_transforms = TransformInstanceLibrary(ani_transforms_path)
    ani_transforms.AddStub("fastani")
    ani_transforms.Save()

Find and open the newly generated stub, which was just created under the folder :python:`ani_transforms_path`.

.. code-block:: python
    :caption: Jupyter
    :linenos:

    ipynbButtonLink(url=ani_transforms_path/"fastani.py")

The stub consists of 4 parts: 

.. code-block:: python
    :caption: fastani.py
    :linenos:

    # First, the Metasmith API is imported.
    from metasmith.python_api import *

    # Second, the contract is defined.
    lib     = TransformInstanceLibrary.ResolveParentLibrary(__file__)
    model   = Transform()
    dep     = model.AddRequirement(lib.GetType("transforms::example input"))
    out     = model.AddProduct(lib.GetType("transforms::example output"))

    # Third, the protocol for executing the tool is defined as a function.
    def protocol(context: ExecutionContext):
        dep_path = context.Input(dep)
        out_path = context.Output(out)
        context.external_shell.Exec(f"touch {out_path.external}")
        return ExecutionResult(
            manifest=[
                {
                    out: out_path.local,
                },
            ],
            success=out_path.local.exists()
        )

    # Fourth, the above components are brought together 
    # to create the actual transform that Metasmith will use.
    TransformInstance(
        protocol=protocol,
        model=model,
        group_by=dep,
    )

The contract
------------------------------------------------------------

Have a look at the contracts of two transforms used in My first agent tutorial.

**getNcbiAssembly**

.. code-block:: python
    :caption: example
    :linenos:
    
    # ...
    lib     = TransformInstanceLibrary.ResolveParentLibrary(__file__)
    model   = Transform()
    dep     = model.AddRequirement(lib.GetType("ncbi::assembly_accession"))
    image   = model.AddRequirement(lib.GetType("containers::ncbi-datasets.oci"))
    fna     = model.AddProduct(lib.GetType("sequences::assembly"))
    faa     = model.AddProduct(lib.GetType("sequences::orfs"))
    gff     = model.AddProduct(lib.GetType("sequences::gff"))
    gbk     = model.AddProduct(lib.GetType("sequences::gbk"))
    # ...

**ppanggolin**

.. code-block:: python
    :caption: example
    :linenos:
    
    # ...
    lib     = TransformInstanceLibrary.ResolveParentLibrary(__file__)
    model   = Transform()
    pan     = model.AddRequirement(lib.GetType("pangenome::pangenome"))
    gbk     = model.AddRequirement(lib.GetType("sequences::gbk"), parents={pan})
    image   = model.AddRequirement(lib.GetType("containers::ppanggolin.oci"))
    matrix  = model.AddProduct(lib.GetType("pangenome::ppanggolin_matrix"))
    pg      = model.AddProduct(lib.GetType("pangenome::ppanggolin_raw"))
    # ...

The :python:`ppanggolin` contract askss for a :python:`sequences::gbk`, which we see is one of the four
products of :python:`getNcbiAssembly`. Both also specify a container image to provide the software tool itself.

.. tip::

    Type names are written as "namespace::type", where the namespace is the name of a collection of related types
    within a :python:`DataTypeLibrary`.

    More on `data types <../usage/data.html>`_ 

Let's take a peek at the :python:`sequences` namespace provided by the standard library.

.. code-block:: python
    :caption: Jupyter
    :linenos:

    ipynbButtonLink(url=MLIB/"data_types/sequences.yml")

.. code-block:: yaml
    :caption: example
    :linenos:

    types:
        assembly:
            properties:
                Format: FASTA
                Data: DNA sequence
                ext: fna
        gbk:
            properties:
                Format: genbank file
                ext: gbk
        # ...

The :python:`assembly` type is described as a fasta file of nucleotide sequences,
which matches the expected input of fastANI. By using :python:`sequences::assembly`,
we would expect fastANI to be able to plug directly into the output of :python:`getNcbiAssembly`
and any other tools in the ecosystem that produce a :python:`sequences::assembly`, without further mental effort.

Let's modify the stub based on what we've learned.

.. code-block:: python
    :caption: fastani.py
    :linenos:

    # ...
    lib     = TransformInstanceLibrary.ResolveParentLibrary(__file__)
    model   = Transform()
    pan     = model.AddRequirement(lib.GetType("pangenome::pangenome"))
    asm     = model.AddRequirement(lib.GetType("sequences::assembly"), parents={pan})
    image   = model.AddRequirement(lib.GetType("ani::fastani.oci"))
    out     = model.AddProduct(lib.GetType("ani::table"))
    # ...

We could add an :python:`ani_table` to the :python:`pangenome` namespace
and the container image :python:`fastani.oci` to the :python:`containers` namespace,
but let's create a new :python:`ani` namespace for the sake of this tutorial.

.. code-block:: python
    :caption: Jupyter
    :linenos:

    ani_types_path = WORKSPACE/"ani_types.yml"
    ani_types_path.touch()
    ipynbButtonLink(url=ani_types_path)

Add the following to the newly created empty file. Since we are not currently
concerned with creating other transforms that will consume the ANI table, 
we will simply create a single property to describe :python:`ani::table`, effectively making it "atomic".
Setting the file extension :python:`ext: tsv` will tell Metasmith to create instances of :python:`ani::table`
in the form of :python:`*.tsv` and is purely cosmetic.

.. code-block:: yaml
    :caption: ani_types.yml
    :linenos:

    types:
        table:
            properties:
                _: average nucleotide identity table
                ext: tsv
        fastani.oci:
            properties:
                _: url for fastani container image

Our new namespace :python:`ani` has two types.

.. code-block:: python
    :caption: Jupyter
    :linenos:

    ani_types = DataTypeLibrary.Load(ani_types_path)
    for name, model in ani_types:
        print(name, model)

    # prints:

    # table <{_:[average nucleotide identity],ext:tsv}:ubCCa4JV>
    # fastani.oci <[url for fastani container image]:BkrOOCzA>

We will need to inform the :python:`TransformInstanceLibrary` of available types.
The following will error because the fastANI contract doesn't agree
with the rest of the transform.

.. code-block:: python
    :caption: Jupyter
    :linenos:

    ani_transforms_path = WORKSPACE/"ani_transforms"
    ani_transforms = TransformInstanceLibrary(ani_transforms_path)
    ani_transforms.AddTypeLibrary(lib=ani_types, namespace="ani")   # new
    ani_transforms.AddTypeLibrary(MLIB/"data_types/sequences.yml")  # new
    ani_transforms.AddTypeLibrary(MLIB/"data_types/pangenome.yml")  # new
    ani_transforms.AddStub("fastani")
    ani_transforms.Save()

The protocol
------------------------------------------------------------

The protocol is executed to fullfill the contract. First, we get instances for each of the
expected and promised data.

.. code-block:: python
    :caption: fastani.py
    :linenos:

    # ...
    def protocol(context: ExecutionContext):
        ipan = context.Input(pan)
        iasm = context.InputGroup(asm)
        iout = context.Output(out)

Next, we have to coerce the input files into what is expected by fastANI. The 
`documentation <https://github.com/ParBLiSS/FastANI>`_ suggests that a pairwise,
all vs all comparison requires a file specifying the file path to each genome per line.
No problem, we can write a bit of python to create the file.

.. code-block:: python
    :caption: fastani.py
    :linenos:

    genomes = "genomes.list"
    with open(genomes, "w") as f:
        for path in iasm:
            f.write(str(path.container)+"\n")

Since :python:`iasm` is an :python:`InputGroup`, the group can be iterated on to work with each element:

.. code-block:: python
    :caption: example
    :linenos:

    for instance in iasm:
        # do something with instance

The most important information held in these instance objects is the path to the given data, or the expected
path to the output data, where:

- :python:`iout.local` is the path within the current protocol
- :python:`iout.external` is the absolute path on the filesystem
- :python:`iout.container` is the path when viewed from inside a container

We can now use :python:`context.ExecWithEnv()` to specify how fastANI 
will be run with the newly created :python:`genomes` file.

.. code-block:: python
    :caption: fastani.py
    :linenos:

    threads = context.params.get('cpus')
    threads = "" if threads is None else f"--threads {threads}"
    context.ExecWithEnv().ifContainerDo(
        env = image,
        cmd = f"""
            fastANI {threads} --queryList {genomes} --refList {genomes} --output {iout.container}
        """,
    )

At the end of the protocol, we will report on the results by returning a manifest of outputs
and indicating success.

.. code-block:: python
    :caption: fastani.py
    :linenos:
    
        return ExecutionResult(
            manifest=[
                {
                    out: iout.local,
                },
            ],
            success=iout.local.exists(),
        )

Create the transform
------------------------------------------------------------

With both the protocol and contract created, we can formally define the transform
for fastANI, along with default resource requests such as cpu, memory, and upper bound for runtime.
The :python:`group_by` parameter specifies how to group consecutive inputs. In this case,
all other inputs will be grouped by the pangenome, which we only expect to affect
:python:`sequence::assembly`.

.. code-block:: python
    :caption: fastani.py
    :linenos:

    # ...
    TransformInstance(
        protocol=protocol,
        model=model, # the contract
        group_by=pan,
        resources=Resources(
            cpus=4,
            memory=Size.GB(8),
            duration=Duration(hours=3),
        )
    )

The full :python:`fastani.py`.

.. code-block:: python
    :caption: fastani.py
    :linenos:
        
    from metasmith.python_api import *

    lib     = TransformInstanceLibrary.ResolveParentLibrary(__file__)
    model   = Transform()
    pan     = model.AddRequirement(lib.GetType("pangenome::pangenome"))
    asm     = model.AddRequirement(lib.GetType("sequences::assembly"), parents={pan})
    image   = model.AddRequirement(lib.GetType("ani::fastani.oci"))
    out     = model.AddProduct(lib.GetType("ani::table"))

    def protocol(context: ExecutionContext):
        ipan = context.Input(pan)
        iasm = context.InputGroup(asm)
        iout = context.Output(out)

        genomes = "genomes.list"
        with open(genomes, "w") as f:
            for path in iasm:
                f.write(str(path.container)+"\n")

        threads = context.params.get('cpus')
        threads = "" if threads is None else f"--threads {threads}"
        context.ExecWithEnv().ifContainerDo(
            env = image,
            cmd = f"""
                fastANI {threads} --queryList {genomes} --refList {genomes} --output {iout.container}
            """,
        )

        return ExecutionResult(
            manifest=[
                {
                    out: iout.local,
                },
            ],
            success=iout.local.exists()
        )

    TransformInstance(
        protocol=protocol,
        model=model, # the contract
        group_by=pan,
        resources=Resources(
            cpus=4,
            memory=Size.GB(8),
            duration=Duration(hours=3),
        )
    )

Testing
============================================================

Our :python:`TransformInstanceLibrary` should now save sucessfully.

.. code-block:: python
    :caption: Jupyter
    :linenos:

    ani_transforms_path = WORKSPACE/"ani_transforms"
    ani_transforms = TransformInstanceLibrary(ani_transforms_path)
    ani_transforms.AddTypeLibrary(lib=ani_types, namespace="ani")
    ani_transforms.AddTypeLibrary(MLIB/"data_types/sequences.yml")
    ani_transforms.AddTypeLibrary(MLIB/"data_types/pangenome.yml")
    ani_transforms.AddStub("fastani")
    ani_transforms.AddStub("ani_heatmap")
    ani_transforms.Save()

.. note::

    The :python:`ani_heatmap` stub is added so the solver can find a
    *downstream* consumer of :python:`ani::table`. We won't run it —
    the default stub body just :python:`touch`-es an output file, which
    is enough for the DAG demonstrations below. A real :python:`ani_heatmap`
    would render an SVG from the all-vs-all similarity table.

    Open the generated :python:`ani_heatmap.py` and replace its contract
    lines with the following (leave the default protocol body and
    :python:`TransformInstance(...)` call untouched):

    .. code-block:: python
        :caption: ani_heatmap.py

        lib   = TransformInstanceLibrary.ResolveParentLibrary(__file__)
        model = Transform()
        ani   = model.AddRequirement(lib.GetType("ani::table"))
        out   = model.AddProduct(lib.GetType("pangenome::heatmap"))

To test fastANI, we will need to prepare inputs and the container image.

.. code-block:: python
    :caption: Jupyter
    :linenos:

    inputs_path = WORKSPACE/"ani_test_inputs.xgdb"
    try:
        inputs = DataInstanceLibrary.Load(inputs_path)
    except:
        inputs = DataInstanceLibrary(inputs_path)
        # add data types
        inputs.AddTypeLibrary(MLIB/"data_types/pangenome.yml")
        inputs.AddTypeLibrary(MLIB/"data_types/ncbi.yml")
        inputs.AddTypeLibrary(ani_types, namespace="ani")

        # register inputs
        group = inputs.AddValue("pangenome", "e coli", "pangenome::pangenome")
        inputs.AddValue("DH10b", "GCF_000019425.1", "ncbi::assembly_accession", parents={group})
        inputs.AddValue("K12", "GCF_000005845.2", "ncbi::assembly_accession", parents={group})
        inputs.AddValue("EPI300", "GCF_049667475.1", "ncbi::assembly_accession", parents={group})
        inputs.AddValue("fastani.oci", "docker://staphb/fastani:1.34", "ani::fastani.oci")
        inputs.Save()

We will prepare resources and transforms using the same method shown in the My first agent tutorial.

.. code-block:: python
    :caption: Jupyter
    :linenos:

    resources = [
        DataInstanceLibrary.Load(MLIB/f"resources/{n}")
        for n in ["containers"]
    ] + [
        view
        for view in inputs.AsSamples("ani::fastani.oci")
    ]

    transforms = [
        TransformInstanceLibrary.Load(MLIB/f"transforms/{n}")
        for n in ["logistics"]
    ] + [
        ani_transforms
    ]

Run it on its own
------------------------------------------------------------

Run the transform by itself before you ask the solver for a workflow. The solver checks that the
contract connects. It does not check that the command line inside the protocol is correct. Only
running the protocol does that, and a whole workflow is a slow way to find a typo.

:python:`msm run` executes one transform against files you supply. It skips the solver and
Nextflow, and it uses the same code that runs a workflow step.

Register the inputs first. ``-i`` names an item in a data instance library, not a path on disk. A
workflow would fetch the genomes through :python:`getNcbiAssembly`, so download two of them
yourself for this test.

.. code-block:: bash
    :caption: Terminal

    $ echo "e coli" > pangenome.txt
    $ echo "docker://staphb/fastani:1.34" > fastani.oci

    $ msm data create ./run_inputs.xgdb --type-lib ./ani_types.yml
    $ msm data add-item ./run_inputs.xgdb --path ./pangenome.txt --dtype pangenome::pangenome
    $ msm data add-item ./run_inputs.xgdb --path ./DH10b.fna --dtype ncbi::assembly
    $ msm data add-item ./run_inputs.xgdb --path ./K12.fna --dtype ncbi::assembly
    $ msm data add-item ./run_inputs.xgdb --path ./fastani.oci --dtype ani::fastani.oci

    $ msm run ani_transforms/fastani.py \
        --agent-home ./msm_home \
        -d ./run_inputs.xgdb \
        -i pan=pangenome.txt \
        -i asm=DH10b.fna \
        -i asm=K12.fna \
        -i image=fastani.oci \
        -w ./fastani_out

Each ``-i`` names the variable the requirement was assigned to in :python:`fastani.py`. That file
declares :python:`pan`, :python:`asm` and :python:`image`. :python:`asm` is repeated because the
protocol reads it with :python:`context.InputGroup`.

The library is the same channel a generated workflow reads, which is why this test tells you
something a bare path could not: an item carries the type you gave it and the parents you recorded,
so a protocol asking :python:`context.SourceOf` gets the answer it will get in a DAG.

Metasmith finds the transform library by walking up from the transform file to
:python:`ani_transforms`. The agent supplies the container runtime, so deploy one first.

Read the products at the end of the run, then edit the protocol and run it again. See
`Transforms <../usage/transforms.html>`_ for the full description of this command.

Upstream chain
------------------------------------------------------------

The first DAG demonstrates how :python:`fastani` plugs into the *existing*
:python:`getNcbiAssembly` transform. We start from NCBI accessions and
target :python:`ani::table`; the solver discovers the chain
:python:`getNcbiAssembly → fastani` automatically.

.. code-block:: python
    :caption: Jupyter
    :linenos:

    agent_home = Source.FromLocal(WORKSPACE/"msm_home")
    smith = Agent(
        home = agent_home,
        runtime=Runtime.DOCKER,
    )

    upstream_targets = TargetBuilder()
    upstream_targets.Add("ani::table")
    task = smith.GenerateWorkflow(
        samples=inputs.AsSamples("ncbi::assembly_accession"),
        resources=resources,
        transforms=transforms,
        targets=upstream_targets,
    )

    upstream_dag = task.plan.RenderDAG(WORKSPACE/"ani_dag_upstream.svg")
    ipynbButtonLink(upstream_dag)

.. figure:: /_static/dag_ani.svg
   :align: center
   :width: 70%
   :alt: the upstream chain — getNcbiAssembly feeds fastani

   :python:`getNcbiAssembly` produces :python:`sequences::assembly`, which
   :python:`fastani` consumes to produce :python:`ani::table`.

Downstream chain
------------------------------------------------------------

The second DAG demonstrates how :python:`fastani` plugs into the *new*
:python:`ani_heatmap` stub. We mock a starting set of
:python:`sequences::assembly` samples and target :python:`pangenome::heatmap`;
the solver discovers the chain :python:`fastani → ani_heatmap`.

.. code-block:: python
    :caption: Jupyter
    :linenos:

    ani_demo_inputs_path = WORKSPACE/"ani_demo_inputs.xgdb"
    try:
        ani_demo_inputs = DataInstanceLibrary.Load(ani_demo_inputs_path)
    except:
        ani_demo_inputs = DataInstanceLibrary(ani_demo_inputs_path)
        ani_demo_inputs.AddTypeLibrary(MLIB/"data_types/pangenome.yml")
        ani_demo_inputs.AddTypeLibrary(MLIB/"data_types/sequences.yml")
        ani_demo_inputs.AddTypeLibrary(ani_types, namespace="ani")

        # mock assemblies — the solver only needs samples to plan a DAG;
        # the values are placeholder paths, never read
        group = ani_demo_inputs.AddValue("pangenome", "e coli", "pangenome::pangenome")
        ani_demo_inputs.AddValue("DH10b", "mock_DH10b.fna", "sequences::assembly", parents={group})
        ani_demo_inputs.AddValue("K12", "mock_K12.fna", "sequences::assembly", parents={group})
        ani_demo_inputs.AddValue("fastani.oci", "docker://staphb/fastani:1.34", "ani::fastani.oci")
        ani_demo_inputs.Save()

    downstream_resources = [
        view for view in ani_demo_inputs.AsSamples("ani::fastani.oci")
    ]
    downstream_targets = TargetBuilder()
    downstream_targets.Add("pangenome::heatmap")
    downstream_task = smith.GenerateWorkflow(
        samples=ani_demo_inputs.AsSamples("sequences::assembly"),
        resources=downstream_resources,
        transforms=transforms,
        targets=downstream_targets,
    )

    downstream_dag = downstream_task.plan.RenderDAG(WORKSPACE/"ani_dag_downstream.svg")
    ipynbButtonLink(downstream_dag)

The rendered DAG shows :python:`fastani` producing :python:`ani::table` and
:python:`ani_heatmap` consuming it to produce :python:`pangenome::heatmap`.

Chaining all three
------------------------------------------------------------

.. tip::

    Targeting :python:`pangenome::heatmap` from :python:`ncbi::assembly_accession`
    sources directly would produce the full chain
    :python:`accession → assembly → ani::table → heatmap` in a single DAG.
    The construction is identical — change only the target on the upstream
    plan above and let the solver discover the full chain. We don't
    demonstrate it here; the two demonstrations above already cover both
    of the new connections in isolation.

Stage and run
------------------------------------------------------------

Finally, we can stage and run the upstream task to actually compute an
ANI table. The following also includes a bit of resource tweaks to let the
three download steps execute concurrently.

.. code-block:: python
    :caption: Jupyter
    :linenos:
        
    smith.StageWorkflow(task, on_exist="update")

    smith.RunWorkflow(
        task,
        config_file=smith.GetNxfConfigPresets()["local"],
        params= dict(
            executor=dict(
                cpus=14,
                queueSize=3, # explicitly set 3 jobs to run in parallel
            ),
            process=dict(
                tries=1,
            ),
        ),
        resource_overrides={
            "*": Resources(
                memory=Size.GB(1),
            ),
            "fastani": Resources(
                cpus=14, # give fastANI all the threads
            )
        }
    )

.. tip::

    It is possible to perform a dry run by setting :python:`stub_delay` to a positive number.
    This will have nextflow execute mock protocols for each process.

    .. code-block:: python
        :linenos:

        smith.RunWorkflow(
            # ...
            stub_delay=3.0,
        )

Once complete, we can have a look at the results.

.. code-block:: python
    :caption: Jupyter
    :linenos:
        
    results_path = smith.GetResultSource(task).GetPath()
    results = DataInstanceLibrary.Load(results_path)

    ipynbButtonLink(results_path/"_metadata/logs.latest/nxf_report.html")

    for path, type_name, endpoint in results.Iterate():
        if path.is_absolute(): continue # inputs have absolute paths
        ipynbButtonLink(results_path/path, f'view {type_name} {path.name}')

Next steps
============================================================

Other tutorials are available in the section panel on the left.
