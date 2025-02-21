ARG CONDA_ENV=for_container

FROM condaforge/miniforge3
ENV DEBIAN_FRONTEND=noninteractive
# scope var from global
ARG CONDA_ENV

# I believe this is to avoid permission issues with 
# manipulating added files to places like /opt
RUN old_umask=`umask` \
    && umask 0000 \
    && umask $old_umask

# Singularity uses tini, but raises warnings
# we set it up here correctly for singularity
ADD ./lib/tini /tini
RUN chmod +x /tini
    
# singularity doesn't use the -s flag, and that causes warnings.
# -g kills process group on ctrl+C
ENTRYPOINT ["/tini", "-s", "-g", "--"]

# globus
RUN apt-get update && apt-get install -y \
    tk tcllib

# https://mamba.readthedocs.io/en/latest/user_guide/mamba.html
# create conda env from yaml config
COPY ./envs/base.yml /opt/base.yml
# use an external cache for solved environments and install packages
RUN --mount=type=cache,target=/opt/conda/pkgs \
    mamba env create -n ${CONDA_ENV} --no-default-packages -f /opt/base.yml
# add bins to PATH so that the env appears "active"
ENV PATH /opt/conda/envs/${CONDA_ENV}/bin:/app:/opt/globusconnectpersonal-latest:$PATH
# install src
COPY ./dist/*.tar.gz /opt/metasmith.tar.gz
RUN pip install /opt/metasmith.tar.gz

COPY ./lib/globusconnectpersonal-latest /opt/globusconnectpersonal-latest
COPY ./main/relay_agent/dist/relay /opt/msm_relay
RUN ln -s /opt/conda/envs/${CONDA_ENV}/lib/python3.12/site-packages/metasmith/bin /app
