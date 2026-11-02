"""The layout engine as it stood at b7c2a2e8, kept runnable beside the new one.

`dag_layout.py` and `dag_draw.py` here are verbatim copies from that commit. They
exist because the bundling work changes what a lane means, and a change to a
drawing cannot be judged from the drawing alone -- you have to put the two
pictures side by side. `ab.py` beside this package is what does that.

The copies are frozen. Fix nothing here; when the new engine is right and this
comparison has stopped earning its keep, delete the directory rather than
letting it drift into a second implementation.
"""

BASELINE_COMMIT = "b7c2a2e8"
