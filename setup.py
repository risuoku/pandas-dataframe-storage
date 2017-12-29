from setuptools import (
    setup,
    find_packages,
)
import importlib

_version = importlib.import_module('pds').__version__
EXCLUDE_FROM_PACKAGES = []

setup (
    name='pandas-dataframe-storage',
    version=_version,
    author='risuoku',
    author_email='risuo.data@gmail.com',
    packages=find_packages(exclude=EXCLUDE_FROM_PACKAGES),
    include_package_data=True,
    install_requires=[
        'pandas',
    ],
)

