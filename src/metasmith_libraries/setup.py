import os
from pathlib import Path
import setuptools

HERE = Path(os.path.realpath(__file__)).parent
NAME = HERE.name
USER = "hallamlab"  # github id
GIT_URL = f"https://github.com/{USER}/{NAME}"
SHORT_SUMMARY = "Hallam Lab transform, data-type, and container library for the Metasmith workflow engine"

with open(HERE / "version.txt") as f:
    VERSION = f.read().strip()

if __name__ == "__main__":
    setuptools.setup(
        name=NAME,
        version=VERSION,
        author="Tony Liu, Ryan McLaughlin, Anika Nag, Aditi Nagaraj, and Steven J. Hallam",
        author_email="shallam@mail.ubc.ca",
        description=SHORT_SUMMARY,
        license_files=("../../LICENSE",),
        url=GIT_URL,
        project_urls={
            "Bug Tracker": f"{GIT_URL}/issues",
        },
        classifiers=[
            "Programming Language :: Python :: 3.12",
            "Operating System :: POSIX :: Linux",
        ],
        packages=["metasmith_libraries"],
        package_dir={"metasmith_libraries": "."},
        package_data={
            "metasmith_libraries": [
                "version.txt",
                "data_types/**",
                "transforms/**",
                "resources/**",
                # a template ships beside the transforms it names, which is what
                # makes its version free -- it cannot be older than the library
                # it was found in
                "templates/**",
                # staged in from envs/metasmith_libraries by
                # `dev/libraries.sh --stage-envs`, gitignored: the repo keeps
                # one directory per module per facet, and setuptools cannot
                # reach outside the package
                "envs/**",
            ],
        },
        # _metadata/ is deliberately absent. It is a build product, and the
        # consumer compiles its own copy (`gui/stdlib.clone_stdlib`), so
        # nothing shipped has to carry it or be kept in step with it. Excluded
        # rather than merely not-listed, because the globs above would sweep it
        # in from whatever state the builder's checkout happened to be in --
        # which makes the wheel's content depend on the builder's scratch, and
        # ships metadata that is stale the moment a transform changes. Every
        # one of them sits at <facet>/<library>/_metadata/, hence the depths.
        # __pycache__ is excluded for the same reason and at every depth the
        # globs above can reach.
        exclude_package_data={
            "metasmith_libraries": [
                "*/*/_metadata/*",
                "*/*/_metadata/*/*",
                "__pycache__/*",
                "*/__pycache__/*",
                "*/*/__pycache__/*",
                "*/*/*/__pycache__/*",
            ],
        },
        python_requires=">=3.12",
        install_requires=[],
    )
