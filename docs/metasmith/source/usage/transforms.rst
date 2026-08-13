Transforms
############################################################

.. role:: python(code)
    :language: python

The manipulation of raw data to interpretable insights typically involves a series of computational steps,
each of which transforms inputs of specific data types to outputs of other data types. Metasmith can be
taught to use computational tools and perform standard protocols by defining transforms.

Similar to how metasmith manages data, a :python:`Transform` specifies the input and output :python:`Endpoints`
for a :python:`TransformInstance`, which contains the explicit computational protocol. :python:`TransformInstances`
are then stored together in a :python:`TransformInstanceLibrary`.

Transform Instance Library
===========================================================

This section will show how to define transforms for 2 commonly used bioinformatics tools, Prodigal[1] and BLAST[2].
Prodigal is a gene prediction tool that identifies `open reading frames (ORFs) <https://en.wikipedia.org/wiki/Open_reading_frame>`_ 
in a given nucleotide sequence. BLAST is a sequence alignment tool that compares a query sequence to a database of sequences.
Together, they can be chained to predict genes within a nucleotide sequence and annotate them with functional information.

Start by creating a :python:`TransformInstanceLibrary` called "simple_genomics". This will create a folder with the same name in
the current directory. 

.. code-block:: python
    :linenos:

    from metasmith import TransformInstanceLibrary
    transforms = TransformInstanceLibrary("./simple_genomics")

At least one :python:`DataTypeLibrary` must be added to define the types of the inputs and outputs of each transform.
The data types provided by the "minimal_genomics" example types library is sufficient for this section.

.. note::

    `How to create a data type library <data.html#data-type-library>`_

.. code-block:: python
    :linenos:

    from metasmith import examples
    dtypes = examples.DataTypeLibraries("minimal_genomics")
    transforms.AddTypeLibrary("genomics", dtypes)

Next, a template is created for each transform.

.. code-block:: python
    :linenos:

    transforms.AddStub("prodigal")
    transforms.AddStub("blast")
    transforms.Save()

At this point, the directory structure of :python:`transforms` should look like this...

.. code-block::

    example.xgdb/
    ├── _metasmith/
    │   ├── index.yml
    │   └── types/
    │       ├── genomics.yml
    │       └── transforms.yml
    ├── blast.py
    └── prodigal.py

... and the contents of :python:`prodigal.py` and :python:`blast.py` should look like this:

.. code-block:: python
    :linenos:

    from pathlib import Path
    from metasmith.python_api import *

    lib = TransformInstanceLibrary.ResolveParentLibrary(__file__)
    model = Transform()
    dep     = model.AddRequirement(node=lib.GetType("transforms::example_input"))
    out     = model.AddProduct(lib.GetType("transforms::example_output"))

    def protocol(context: ExecutionContext):
        out_path = context.Get(out)
        context.external_shell.Exec(f"touch {out_path.external}")
        return ExecutionResult(success=out_path.local.exists())

    TransformInstance(
        protocol = protocol,
        model = model,
        output_signature = {
            out: "output.txt",
        },
        resources = Resources(
            cpus = 4,
            memory = Size.GB(16),
            duration = Duration(days=1, hours=12, minutes=30),
        )
    )

- Data types are provided by the parent library, which is obtained on line 4
- lines 5-7 define the input and output data types for the transform.
- Data types are referred to by "namespace:type"
- The computational protocol where either prodigal or blast will be executed starts on line 9
- This definition is published to the library by creating a :python:`TransformInstance` on line 14
- Note that the output file name is specified on line 18

Execution context
-----------------------------------------------------------

A :python:`ExecutionContext` object will be provided to the protocol at runtime. It will contain:

- the paths to inputs and expected outputs :python:`context.Get(lib.GetType("namespace::type"))`
- access to a `bash <https://en.wikipedia.org/wiki/Bash_(Unix_shell)>`_ terminal environment :python:`context.external_shell`
- a declaration of how the tool runs in each world :python:`context.ExecWithEnv().ifContainerDo(env, cmd).ifVirtualEnvDo(env, cmd)`
- and additional parameters like CPU and memory limits :python:`context.params`

Prodigal Example
-----------------------------------------------------------

Prodigal accepts a contiguous nucleotide sequence or "contig" and produces a list of predicted ORFs as amino acid sequences in fasta format.
Additionally, the container providing the prodigal itself is specified as an input. The syntax for running prodigal can be obtained
from studying its `documentation <https://github.com/hyattpd/prodigal/wiki/Gene-Prediction-Modes#normal-mode>`_.

.. code-block:: python
    :linenos:

    from pathlib import Path
    from metasmith.python_api import *

    lib = TransformInstanceLibrary.ResolveParentLibrary(__file__)
    model = Transform()
    contigs = model.AddRequirement(node=lib.GetType("genomics::contigs"))
    image   = model.AddRequirement(node=lib.GetType("genomics::oci_image_prodigal"))
    orfs    = model.AddProduct(lib.GetType("genomics::aa_sequences"))

    def protocol(context: ExecutionContext):
        out_path = context.Get(orfs)
        context.ExecWithEnv().ifContainerDo(
            env = image,
            cmd = f"""\
                prodigal \
                    -i {context.Get(contigs).container} \
                    -a {out_path.container} \
                    -f gff \
                    -o {out_path.container.with_suffix('.gff')} \
                """,
        )
        return ExecutionResult(success=out_path.local.exists())

    TransformInstance(
        protocol = protocol,
        model = model,
        output_signature = {
            orfs: "orfs.faa",
        },
    )

BLAST Example
-----------------------------------------------------------

Similarly, BLAST requires a query sequence and a database of sequences to compare against. For simplicity,
another fasta file will be used instead of a true BLAST database. The syntax for running BLAST can also be 
obtained from studying its `documentation <https://www.ncbi.nlm.nih.gov/books/NBK279690/>`_.

.. code-block:: python
    :linenos:

    from pathlib import Path
    from metasmith.python_api import *

    lib = TransformInstanceLibrary.ResolveParentLibrary(__file__)
    model = Transform()
    orfs    = model.AddRequirement(node=lib.GetType("genomics::aa_sequences"))
    refdb   = model.AddRequirement(node=lib.GetType("genomics::protein_reference_fasta"))
    image   = model.AddRequirement(node=lib.GetType("genomics::oci_image_blast"))
    annot   = model.AddProduct(lib.GetType("genomics::orf_annotations"))

    def protocol(context: ExecutionContext):
        out_path = context.Get(annot)
        COLUMNS = f"qseqid sseqid bitscore evalue pident"
        context.ExecWithEnv().ifContainerDo(
            env = image,
            cmd = f"""\
                blastp \
                    -query {context.Get(orfs).container} \
                    -subject {context.Get(refdb).container} \
                    -outfmt "6 {COLUMNS}" \
                    -out {out_path.container} \
                """,
        )
        return ExecutionResult(success=out_path.local.exists())

    TransformInstance(
        protocol = protocol,
        model = model,
        output_signature = {
            annot: "annotations.csv",
        },
    )

References
===========================================================

1.	Hyatt D, Chen GL, LoCascio PF, Land ML, Larimer FW, Hauser LJ. `Prodigal: prokaryotic gene recognition and translation initiation site identification <https://doi.org/10.1186/1471-2105-11-119>`_. BMC Bioinformatics. 2010;11(1):119. doi:10.1186/1471-2105-11-119
2.	Altschul SF, Gish W, Miller W, Myers EW, Lipman DJ. `Basic local alignment search tool <https://doi.org/10.1016/S0022-2836(05)80360-2>`_. J Mol Biol. 1990;215(3):403–10. doi:10.1016/S0022-2836(05)80360-2
