import os, sys
from pathlib import Path
HERE = Path(os.path.realpath(__file__)).parent
_src = str(HERE.parent)
sys.path = [_src] + [p for p in sys.path if p != _src]
import setuptools
from fabfos.constants import USER, NAME, VERSION, SHORT_SUMMARY, ENTRY_POINTS, GIT_URL

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
