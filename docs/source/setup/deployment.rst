.. role:: python(code)
   :language: python

Deploying agents
############################################################

Locally
============================================================

asdf

with SSH
============================================================

asdf


.. _Container Runtime:
Container Runtime
------------------------------------------------------------

To maximize reproducibility and portability, Metasmith relies on `OCI <https://en.wikipedia.org/wiki/Open_Container_Initiative>`_
compliant containers to standardize the compute environment for itself and the tools that it runs. One of the following must be installed:

- `Apptainer <https://apptainer.org/>`_ is recommended due to its compatibility with research compute infrastructure.
- `Docker <https://docs.docker.com/get-docker/>`_ (experimental). On Linux, please also `do this to enable docker without "sudo" <https://docs.docker.com/engine/install/linux-postinstall/>`_
