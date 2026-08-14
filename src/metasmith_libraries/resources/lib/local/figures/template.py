from plotly import subplots as sp, graph_objs as go

def SubplotSize(fig, row, col, ncols):
    # Determine axis prefix (e.g., '' for first, '2' for second)
    axis_idx = (row - 1) * ncols + col
    xaxis_name = 'xaxis' if axis_idx == 1 else f'xaxis{axis_idx}'
    yaxis_name = 'yaxis' if axis_idx == 1 else f'yaxis{axis_idx}'

    # Extract domains
    x_domain = getattr(fig.layout, xaxis_name).domain
    y_domain = getattr(fig.layout, yaxis_name).domain

    # Get figure dimensions and margins
    width = fig.layout.width
    height = fig.layout.height
    margin = fig.layout.margin
    l = margin.l if margin else 0
    r = margin.r if margin else 0
    t = margin.t if margin else 0
    b = margin.b if margin else 0

    # Calculate pixel dimensions
    plot_width = width - l - r
    plot_height = height - t - b
    subplot_width = (x_domain[1] - x_domain[0]) * plot_width
    subplot_height = (y_domain[1] - y_domain[0]) * plot_height

    return subplot_width, subplot_height

def BaseFigure(shape: tuple[int, int]=(1, 1), **kwargs) -> go.Figure:
    ncols, nrows = shape
    # column_widths=[0.1, 0.9], row_heights=[0.3, 0.1, 0.6],
    params: dict = dict(
        rows=nrows, cols=ncols,
        horizontal_spacing=0.02, vertical_spacing=0.02,
        shared_yaxes=True, shared_xaxes=True,
    ) | kwargs
    return sp.make_subplots(**params)

def ApplyTemplate(fig: go.Figure, default_xaxis: dict = dict(), default_yaxis: dict = dict(), axis: dict[str, dict] = dict(), layout: dict = dict()):
    # @axis
    # example: {"1 1 y": dict(showticklabels=True, categoryorder='array', categoryarray=cat_list)}
    # params: https://plotly.com/python/reference/layout/xaxis/

    color_none = 'rgba(0,0,0,0)'
    color_axis = 'rgba(0, 0, 0, 0.15)'
    axis_template = dict(showgrid=False, showticklabels=True, linecolor="black", linewidth=1, ticks="outside", gridcolor=color_axis, zerolinecolor=color_none, zerolinewidth=1)
    DEF_XAXIS: dict = axis_template|default_xaxis
    DEF_YAXIS: dict = axis_template|default_yaxis
    logged_cols, logged_rows = [], []
    _original_layout = fig.layout.__dict__
    _layout = layout.copy()
    _rows, _ncols = fig._get_subplot_rows_columns()
    nrows, ncols = [len(x) for x in [_rows, _ncols]]
    for i in range(nrows*ncols):
        x, y = i%ncols + 1, i//ncols + 1
        i += 1
        ax = DEF_XAXIS | axis.get(f"{x} {y} x", DEF_XAXIS.copy())
        ay = DEF_YAXIS | axis.get(f"{x} {y} y", DEF_YAXIS.copy())
        if x in logged_cols: ax |= dict(type="log")
        if y in logged_rows: ay |= dict(type="log")
        kx = f"xaxis{i if i != 1 else ''}"
        ky = f"yaxis{i if i != 1 else ''}"
        _layout[kx] = _layout.get(kx, {})|ax
        _layout[ky] = _layout.get(ky, {})|ay
    
    bg_col="white"
    W, H = 1000, 600
    _layout: dict = dict(
        width=W, height=H,
        paper_bgcolor=bg_col,
        plot_bgcolor=bg_col,
        margin=dict(
            l=15, r=15, b=15, t=15, pad=5
        ),
        font=dict(
            family="Arial",
            size=16,
            color="black",
        ),
        legend=dict(
            font=dict(
                size=12,
            ),
        ),
    ) | _layout
    fig.update_layout(**_layout)
    return fig
