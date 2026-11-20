# The Hallam lab's Biofactorial contaminant list, upstream ASPIRE's `biof_db`
# (`ref_db/ssu_pipeline_contaminants`). It has no public home yet: upstream names it only as a
# path on its maintainer's machine. Until SOURCE names one, this transform fails and a driver
# gives `aspire::contaminant_reference_set` directly.
#
# SOURCE is a FASTA (optionally gzipped) or a tarball of the BLAST database. A database is
# turned back into FASTA with `blastdbcmd -entry all`, as upstream's PREPARE_BLAST_DATABASES
# does.

from metasmith.python_api import *

lib    = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model  = Transform()
python = model.AddRequirement(lib.GetType("env::python_for_data_science.env"))
blast  = model.AddRequirement(lib.GetType("env::blast.env"))
biof   = model.AddProduct(lib.GetType("aspire::contaminants_biofactorial"))

SOURCE = ""


def protocol(context: ExecutionContext):
    ibiof = context.Output(biof)
    if not SOURCE:
        context.ExecWithEnv(env=python, cmd="""\
            echo "downloadBiofactorialContaminants: SOURCE is empty. The Biofactorial list has no public home yet; give aspire::contaminant_reference_set instead." >&2
            exit 1
        """)
        return ExecutionResult(manifest=[{biof: ibiof.local}], success=False)

    name = SOURCE.rsplit("/", 1)[-1]
    context.ExecWithEnv(env=python, cmd=f"""\
        set -euo pipefail
        python3 -c "import urllib.request; urllib.request.urlretrieve('{SOURCE}', '{name}')"
    """)
    context.ExecWithEnv(env=blast, cmd=f"""\
        set -euo pipefail
        case {name} in
            *.tar.gz|*.tgz)
                mkdir -p db && tar -xzf {name} -C db
                index=$(find db -name '*.nal' -print -quit)
                [ -n "$index" ] || index=$(find db -name '*.nsq' -print -quit)
                blastdbcmd -db "${{index%.*}}" -entry all > {ibiof.container} ;;
            *.gz) gzip -cd {name} > {ibiof.container} ;;
            *) cp {name} {ibiof.container} ;;
        esac
        rm -rf {name} db
        grep -q '^>' {ibiof.container}
    """)

    return ExecutionResult(
        manifest=[{biof: ibiof.local}],
        success=ibiof.local.exists() and ibiof.local.stat().st_size > 0,
    )


TransformInstance(
    protocol=protocol,
    model=model,
    group_by=python,
    labels=["local"],
    resources=Resources(
        cpus=1,
        memory=Size.GB(4),
        duration=Duration(hours=1),
    ),
)
