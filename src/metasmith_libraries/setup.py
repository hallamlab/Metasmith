import os
from pathlib import Path
import setuptools

HERE = Path(os.path.realpath(__file__)).parent
NAME = HERE.name
USER = "hallamlab"
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
                "templates/**",
                "envs/**",
            ],
        },
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
