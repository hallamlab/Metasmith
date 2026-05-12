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

We can now use :python:`context.ExecWithContainer(...)` to specify how fastANI 
will be run with the newly created :python:`genomes` file.

.. code-block:: python
    :caption: fastani.py
    :linenos:

    threads = context.params.get('cpus')
    threads = "" if threads is None else f"--threads {threads}"
    context.ExecWithContainer(
        image = image,
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
        context.ExecWithContainer(
            image = image,
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
    ani_transforms.Save()

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

Let's generate the workflow and inspect the plan.

.. code-block:: python
    :caption: Jupyter
    :linenos:

    agent_home = Source.FromLocal(WORKSPACE/"msm_home")
    smith = Agent(
        home = agent_home,
        runtime=ContainerRuntime.DOCKER,
    )

    targets = TargetBuilder()
    targets.Add("ani::table")
    task = smith.GenerateWorkflow(
        samples=inputs.AsSamples("ncbi::assembly_accession"),
        resources=resources,
        transforms=transforms,
        targets=targets,
    )

    dag = task.plan.RenderDAG(WORKSPACE/"ani_dag.svg")
    ipynbButtonLink(dag)

.. figure:: /_static/dag_ani.svg
   :align: center
   :width: 70%
   :alt: the generated fastANI workflow

Finally, we can stage and run fastANI. The following also includes a bit of
resource tweaks to let the three download steps execute concurrently. 

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
