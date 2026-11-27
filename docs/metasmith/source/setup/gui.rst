Web GUI
############################################################

The web GUI runs a workflow from a shipped template without writing Python. It covers the same
steps as the notebook API: ssh host, agent, data, plan, run and results.

Start it from an empty workspace folder. On first start it copies the tutorials and the standard
library into that folder, so start it from the same folder each time.

.. code-block:: console
    :caption: Terminal

    $ mkdir -p metasmith_ws
    $ cd metasmith_ws
    $ msm gui

The page opens at http://127.0.0.1:8090. It binds to the loopback interface only. Run
``msm gui --help`` for the port, the host and the project folder.

1. Pick a template. The template list shows each template's description.
2. Give each sample a file of the template's sample type.
3. Deselect any target you do not need.
4. Solve. The GUI shows the plan as a graph of steps.
5. Run it on an agent, then follow its progress and open its results.

Read :doc:`../workflows/_index` for what each shipped template does.
