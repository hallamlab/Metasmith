import os, sys
from pathlib import Path
HERE = Path(os.path.realpath(__file__)).parent
sys.path = [str(p) for p in set([
    HERE.joinpath("src")
]+sys.path)]
import setuptools
from metasmith.constants import NAME, VERSION, SHORT_SUMMARY, ENTRY_POINTS, GIT_URL

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

if __name__ == "__main__":
    setuptools.setup(
        name=NAME,
        version=VERSION,
        author="Tony Liu, Ryan McLaughlin, Anika Nag, Aditi Nagaraj, and Steven J. Hallam",
        author_email="shallam@mail.ubc.ca",
        description=SHORT_SUMMARY,
        long_description=long_description,
        long_description_content_type="text/markdown",
        license_files = ('LICENSE',),
        url=f"{GIT_URL}",
        project_urls={
            "Bug Tracker": f"{GIT_URL}/issues",
        },
        classifiers=[
            "Programming Language :: Python :: 3.12",
            "Operating System :: POSIX :: Linux",
        ],
        package_dir={"": "src"},
        packages=setuptools.find_packages(where="src"),
        package_data={
            "":[ # "" is all packages
                "version.txt",
                "nextflow_config/*",
                "bin/*",
            ],
            # examples
            # "package-name": ["*.txt"],
            # "test_package": ["res/*.txt"],
        },
        entry_points={
            'console_scripts': ENTRY_POINTS,
        },
        python_requires=">=3.12",
        install_requires=[
        ]
    )