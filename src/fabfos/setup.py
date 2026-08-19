import os
from pathlib import Path
import setuptools

HERE = Path(os.path.realpath(__file__)).parent
NAME = HERE.name
USER = "hallamlab"
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
                "_library/**/*",
                "algorithm/*.py",
                "algorithm/_metadata/**/*",
            ],
        },
        entry_points={"console_scripts": ENTRY_POINTS},
        python_requires=">=3.12",
        install_requires=[],
    )
