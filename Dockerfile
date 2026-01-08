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

# singularity doesn't use the -s flag, and that causes warnings.
# -g kills process group on ctrl+C
ENTRYPOINT ["/tini", "-s", "-g", "--"]

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        openssh-client \
        tmux \
    && rm -rf /var/lib/apt/lists/*

# https://mamba.readthedocs.io/en/latest/user_guide/mamba.html
# create conda env from yaml config
COPY ./envs/base.yml /opt/base.yml
# use an external cache for solved environments and install packages
RUN --mount=type=cache,target=/opt/conda/pkgs \
    mamba env create -n ${CONDA_ENV} -f /opt/base.yml
# add bins to PATH so that the env appears "active"
ENV PATH=/opt/conda/envs/${CONDA_ENV}/bin:/app:/opt/globusconnectpersonal-latest:$PATH

# install src
COPY ./dist/*.tar.gz /opt/metasmith.tar.gz
RUN pip install /opt/metasmith.tar.gz

COPY ./lib/globusconnectpersonal-latest /opt/globusconnectpersonal-latest
COPY ./main/relay_agent/target/x86_64-unknown-linux-musl/release/msm_relay /app/msm_relay.x86_64-linux
COPY ./main/relay_agent/target/aarch64-unknown-linux-musl/release/msm_relay /app/msm_relay.arm64-linux
COPY ./main/relay_agent/target/x86_64-apple-darwin/release/msm_relay /app/msm_relay.x86_64-darwin
COPY ./main/relay_agent/target/aarch64-apple-darwin/release/msm_relay /app/msm_relay.arm64-darwin
RUN ln -s /opt/conda/envs/${CONDA_ENV}/lib/python3.12/site-packages/metasmith/bin/* /app

EXPOSE 8080
# WORKDIR /workspace
# COPY ./metasmith_starter.ipynb /workspace/metasmith_starter.ipynb
# RUN touch /workspace/empty
# RUN pip install jupyterlab jupyterlab-lsp python-lsp-server[all]

# CMD ["bash","-lc","jupyter lab \
#     --ip=127.0.0.1 \
#     --port=8080 \
#     --allow-root \
#     --no-browser \
#     --LabApp.default_url='/lab/tree/metasmith_starter.ipynb'"]
