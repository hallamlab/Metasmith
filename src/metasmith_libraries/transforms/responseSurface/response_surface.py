from metasmith.python_api import *

lib = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()

image   = model.AddRequirement(lib.GetType("env::python_for_data_science.env"))
local   = model.AddRequirement(lib.GetType("lib::local"))
script  = model.AddRequirement(lib.GetType("lib::response_surface.py"))
data    = model.AddRequirement(lib.GetType("media_optimization::growth_data"))

out_coefficients     = model.AddProduct(lib.GetType("media_optimization::response_surface_coefficients"))
out_suggestions      = model.AddProduct(lib.GetType("media_optimization::model_suggestions"))
out_crashed          = model.AddProduct(lib.GetType("media_optimization::crashed_cultures"))
out_growth_plot      = model.AddProduct(lib.GetType("media_optimization::growth_characteristics_plot"))
out_importance_plot  = model.AddProduct(lib.GetType("media_optimization::factor_importance_plot"))
out_suggestions_plot = model.AddProduct(lib.GetType("media_optimization::model_suggestions_plot"))
out_rs1d_plot        = model.AddProduct(lib.GetType("media_optimization::response_surface_1d_plot"))
out_rs2d_plot        = model.AddProduct(lib.GetType("media_optimization::response_surface_2d_plot"))

def protocol(context: ExecutionContext):
    idata   = context.Input(data)
    iscript = context.Input(script)

    ocoef   = context.Output(out_coefficients)
    osugg   = context.Output(out_suggestions)
    ocrash  = context.Output(out_crashed)
    ogrowth = context.Output(out_growth_plot)
    oimp    = context.Output(out_importance_plot)
    osplot  = context.Output(out_suggestions_plot)
    ors1d   = context.Output(out_rs1d_plot)
    ors2d   = context.Output(out_rs2d_plot)

    # Create fake home for kaleido/plotly browser deps
    context.LocalShell("""
        mkdir -p ./fake_home/.cache
        mkdir -p ./fake_home/.local
        mkdir -p ./fake_home/.config
        mkdir -p ./fake_home/.pki
    """)

    context.ExecWithEnv().ifContainerDo(
        env=image,
        binds=[
            ("$(pwd -P)/fake_home/.cache",  "$HOME/.cache"),
            ("$(pwd -P)/fake_home/.local",  "$HOME/.local"),
            ("$(pwd -P)/fake_home/.config", "$HOME/.config"),
            ("$(pwd -P)/fake_home/.pki",    "$HOME/.pki"),
        ],
        cmd=f"""\
            export NUMBA_CACHE_DIR=$TMPDIR
            python {iscript.container} \
                {idata.container} \
                rs_output
        """,
    )

    # Copy outputs from rs_output/ to typed output locations
    context.LocalShell(f"cp rs_output/response_surface_coefficients.csv {ocoef.local}")
    context.LocalShell(f"cp rs_output/model_suggestions.csv {osugg.local}")
    context.LocalShell(f"cp rs_output/crashed_cultures.csv {ocrash.local}")
    context.LocalShell(f"cp rs_output/growth_characteristics.svg {ogrowth.local}")
    context.LocalShell(f"cp rs_output/top_10_factor_importance.svg {oimp.local}")
    context.LocalShell(f"cp rs_output/model_suggestions.svg {osplot.local}")
    context.LocalShell(f"cp rs_output/response_surface.svg {ors1d.local}")
    context.LocalShell(f"cp rs_output/response_surface2.svg {ors2d.local}")

    return ExecutionResult(
        manifest=[{
            out_coefficients: ocoef.local,
            out_suggestions: osugg.local,
            out_crashed: ocrash.local,
            out_growth_plot: ogrowth.local,
            out_importance_plot: oimp.local,
            out_suggestions_plot: osplot.local,
            out_rs1d_plot: ors1d.local,
            out_rs2d_plot: ors2d.local,
        }],
        success=ocoef.local.exists() and osugg.local.exists(),
    )

TransformInstance(
    protocol=protocol,
    model=model,
    group_by=data,
    resources=Resources(
        cpus=2,
        memory=Size.GB(8),
        duration=Duration(hours=1),
    ),
)
