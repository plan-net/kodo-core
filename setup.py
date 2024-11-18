from setuptools import setup, find_packages

import kodo

setup(
    name='kodo',
    version=kodo.__version__,
    packages=find_packages(exclude=['docs*', 'tests*']),
    include_package_data=True,
)