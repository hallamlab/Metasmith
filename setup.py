import os, sys
from pathlib import Path
HERE = Path(os.path.realpath(__file__)).parent
_src = str(HERE.joinpath("src"))
sys.path = [_src] + [p for p in sys.path if p != _src]
import setuptools
from metasmith.constants import USER, NAME, VERSION, FULL_VERSION, SHORT_SUMMARY, ENTRY_POINTS, GIT_URL

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

if __name__ == "__main__":
    setuptools.setup(
        name=NAME,
        version=FULL_VERSION,
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
        packages=setuptools.find_packages(
            where="src",
            exclude=[
                "metasmith_libraries", "metasmith_libraries.*",
                "ecspr", "ecspr.*",
                "fabfos", "fabfos.*",
            ],
        ),
        package_data={
            "":[
                "version.txt",
                "build_hash.txt",
                "nextflow_config/**",
                "bin/**",
                "example_resources/**",
                "jupyter_lab/**",
                "gui/static/**",
                "gui/icon/**",
                "engine/**",
                # the vendored standard library, staged by
                # `dev/metasmith.sh --vendor-library` and never committed. It is
                # content, not code, so find_packages cannot see it and nothing
                # but this line ships it. Inside src/metasmith/ on purpose:
                # `_build_hash.py` walks that tree, so the engine's build hash
                # covers the library's content and the two cannot drift.
                "vendor/**",
            ],
        },
        entry_points={
            'console_scripts': ENTRY_POINTS,
        },
        python_requires=">=3.12",
        install_requires=[],
    )
