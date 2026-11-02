from metasmith.python_api import *

lib   = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()
image = model.AddRequirement(lib.GetType("env::carveme.env"))
out   = model.AddProduct(lib.GetType("modelling::carveme_universe"))

def protocol(context: ExecutionContext):
    iout = context.Output(out)

    # `universe_bacteria.xml.gz` is CarveMe's own default (`config.cfg`'s
    # `[generated] default_universe`); `universe_gramneg.xml.gz` is the same shape
    # over 64 Growth metabolites instead of 57 and has no producer here -- nothing
    # downstream needs it yet, and adding it is a second AddProduct + a `--gramneg`
    # branch on this same command whenever something does.
    _cmd = f"""\
            python -c "
from carveme import config, project_dir
import shutil
shutil.copy(project_dir + config.get('generated', 'default_universe'), '{iout.container}')
"
        """
    context.ExecWithEnv(env=image, cmd=_cmd)

    return ExecutionResult(
        manifest=[{out: iout.local}],
        success=iout.local.exists(),
    )

TransformInstance(
    protocol=protocol,
    model=model,
    group_by=image,
    resources=Resources(
        cpus=1,
        memory=Size.GB(2),
        duration=Duration(minutes=10),
    ),
)
