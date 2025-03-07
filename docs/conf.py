# Configuration file for the Sphinx documentation builder.

# -- Project information

from pathlib import Path
WORKSPACE_ROOT = Path(__file__).resolve().parents[1]
with open(WORKSPACE_ROOT/'src/metasmith/version.txt') as f:
    version_string = f.read().strip()

project = 'Metasmith'
copyright = '2025, Hallam Lab'
author = 'Tony X. Liu, Steven Hallam'

release = version_string
version = version_string

# -- General configuration

extensions = [
    'sphinx.ext.duration',
    'sphinx.ext.doctest',
    'sphinx.ext.autodoc',
    'sphinx.ext.autosummary',
    'sphinx.ext.intersphinx',
]

intersphinx_mapping = {
    'python': ('https://docs.python.org/3/', None),
    'sphinx': ('https://www.sphinx-doc.org/en/master/', None),
}
intersphinx_disabled_domains = ['std']

templates_path = ['_templates']

# -- Options for HTML output

html_theme = 'sphinx_rtd_theme'

# -- Options for EPUB output
epub_show_urls = 'footnote'