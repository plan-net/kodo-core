from setuptools import find_packages, setup

import kodo

setup(
    name='kodo',
    version=kodo.__version__,
    packages=find_packages(exclude=['docs*', 'tests*']),
    include_package_data=True,
    install_requires=[
        "requests",
        "click",
        "pyyaml",
        "crewai",
        "crewai_tools"
    ],
    entry_points={
        'console_scripts': [
            'creco = kodo.koco:cli',
        ],
    },
    extras_require={
        "tests": [
            "pytest",
            "pytest-cov",
            "isort",
            "autopep8"
        ]
    }
)
