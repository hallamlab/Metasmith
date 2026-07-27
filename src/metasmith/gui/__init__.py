"""The metasmith GUI: a localhost web page over the same operations the CLI uses.

`msm gui` starts a Flask server rooted at the working directory and serves a
single-page frontend covering the full run path -- host, agent, inputs, plan,
run, results -- without writing any Python. Transform *authoring* is deliberately
absent: that is a developer action and stays in the notebook and CLI.

Layout of this package:

    store.py       the project directory: agents, workflows, runs
    names.py       readable names ("blazing-ape", "blazing-ape-0XwE9")
    stdlib.py      the standard library clone, shared with `msm lab`
    sshconfig.py   the managed block in the user's ~/.ssh/config
    jobs.py        background work and its log streams
    watcher.py     re-attaching to runs the server did not start
    api.py         the REST surface
    app.py         the Flask application
    static/        the built frontend bundle (generated, never committed)
"""
from .app import create_app, serve

__all__ = ["create_app", "serve"]
