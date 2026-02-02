from IPython.display import display, SVG, HTML
from pathlib import Path

def ipynbButtonLink(url, text: str|None=None, color: str="#1976D2", on_hover_color: str="#1565C0", size: str="16px", external=False):
    if not external:
        url = Path(url).relative_to(Path(".").absolute(), walk_up=True)
        if text is None:
            text = url.name
    else:
        if text is None:
            text = url

    # font-weight: bold;
    html_button = f"""
    <style>
        .custom-button {{
            /* Button Appearance */
            background-color: {color};
            padding: 10px 20px;
            border-radius: 4px; 
            border: none;
            box-shadow: 0 2px 2px 0 rgba(0,0,0,0.14); 
            
            /* Link/Text Appearance Overrides */
            color: white !important; /* Forces text color to white */
            text-decoration: none; /* Removes the underline */
            font-size: {size};

            /* Positioning/Behavior */
            text-align: center;
            display: inline-block;
            margin: 4px 2px;
            cursor: pointer;
        }}
        .custom-button:hover {{
            background-color: {on_hover_color}; /* Darker blue on hover */
        }}
    </style>
    <a href="{url}" target="_blank" class="custom-button">{text}</a>
    """
    display(HTML(html_button))
