import os
from pathlib import Path
import setuptools

HERE = Path(os.path.realpath(__file__)).parent
NAME = HERE.name
USER = "hallamlab"  # github id
GIT_URL = f"https://github.com/{USER}/{NAME}"
SHORT_SUMMARY = "A pipeline for the analysis of pooled fosmid data, run on metasmith"
ENTRY_POINTS = [f"{e}={NAME}.cli:main" for e in (NAME, "ffs")]

with open(HERE / "version.txt") as f:
    VERSION = f.read().strip()

if __name__ == "__main__":
    setuptools.setup(
        name=NAME,
        version=VERSION,
        author="Tony Liu, Connor Morgan-Lang, Avery Noonan, Zach Armstrong, and Steven J. Hallam",
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
        packages=["fabfos", "fabfos.pipelines"],
        package_dir={"fabfos": "."},
        package_data={
            "fabfos": [
                "version.txt",
                # the bundled metasmith library shipped with conda installs
                "_library/**/*",
                # FabFos's own algorithm library. Neither directory is a python
                # package, so find_packages does not see them and they only ship
                # because of these globs. `algorithm/_metadata/**` is the part
                # that matters: without it the directory loads as a resource
                # library and then resolves nothing.
                "algorithm/*.py",
                "algorithm/_metadata/**/*",
            ],
        },
        entry_points={"console_scripts": ENTRY_POINTS},
        python_requires=">=3.12",
        # metasmith provides the planner/executor; it is a conda dependency
        # (see conda_recipe), not a pip one.
        install_requires=[],
    )
