#!/usr/bin/env python3

from pathlib import Path

from setuptools import find_packages, setup


ROOT = Path(__file__).parent


def get_version():
    namespace = {}
    exec((ROOT / "kahi_unicity_person_graph" /
         "_version.py").read_text(), namespace)
    return namespace["__version__"]


setup(
    name="Kahi_unicity_person_graph",
    version=get_version(),
    author="Colav",
    author_email="colav@udea.edu.co",
    packages=find_packages(exclude=["tests"]),
    include_package_data=True,
    url="https://github.com/colav/Kahi_plugins",
    license="BSD",
    description="Collision-safe graph-based person unicity plugin for Kahi",
    long_description=(ROOT / "README.md").read_text(),
    long_description_content_type="text/markdown",
    install_requires=[
        "kahi",
        "pymongo",
        "joblib",
        "kahi_impactu_utils",
    ],
)
