from pathlib import Path
WORKSPACE_ROOT = Path(__file__).resolve().parents[3]
with open(WORKSPACE_ROOT/'src/metasmith/version.txt') as f:
    version_string = f.read().strip()
import sys
sys.path = list({str(x) for x in [
    WORKSPACE_ROOT/"src/metasmith"
]+sys.path})
from constants import GIT_URL, USER, NAME

project = NAME.title()
copyright = '2025, Hallam Lab'

release = version_string
version = version_string


extensions = [
    'sphinx.ext.duration',
    'sphinx.ext.doctest',
    'sphinx.ext.autodoc',
    'sphinx.ext.autosummary',
    'sphinx.ext.intersphinx',
    'sphinx_copybutton',
    'myst_nb',
    'sphinx_design',
]

intersphinx_mapping = {
    'python': ('https://docs.python.org/3/', None),
    'sphinx': ('https://www.sphinx-doc.org/en/master/', None),
}
intersphinx_disabled_domains = ['std']
templates_path = ['_templates']

html_theme = 'pydata_sphinx_theme'
html_static_path = ['_static']
html_title = f"{project} {version}"
html_css_files = [
    'theme_overrides.css',
]
html_sidebars = {
    '**': ['sidebar-nav-bs.html'],
}

html_theme_options = {
    "secondary_sidebar_items": {
        "**": ["page-toc"],
        "index": [],
    },
    "navbar_align": "left",
    "show_prev_next": False,
    "icon_links": [
        {
            "name": "GitHub",
            "url": GIT_URL,
            "icon": "fa-brands fa-github",
            "type": "fontawesome",
        },
        {
            "name": "Conda",
            "url": f"https://anaconda.org/{USER}/{NAME}",
            "icon": "_static/Anaconda.svg",
            "type": "local",
        },
        {
            "name": "Docker",
            "url": f"https://quay.io/repository/{USER}/{NAME}",
            "icon": "fa-brands fa-docker",
            "type": "fontawesome",
        },
   ],
   
}

epub_show_urls = 'footnote'

nb_execution_mode = "off"
nb_remove_code_outputs = True

copybutton_prompt_text = "$ "
