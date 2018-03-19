#!/usr/bin/env python
"""See <https://setuptools.readthedocs.io/en/latest/>.
"""
from setuptools import setup, find_packages

setup(


    # ┏━━━━━━━━━━━━━━━━━━━━━━┓
    # ┃ Publication Metadata ┃
    # ┗━━━━━━━━━━━━━━━━━━━━━━┛
    version='1.0.0',
    name='timetell-importer',
    description="Importer of timetell data",
    # TODO:
    # long_description="""
    #
    # """,
    url='https://github.com/Amsterdam/timetell',
    author='Amsterdam Datapunt',
    author_email='datapunt@amsterdam.nl',
    license='Mozilla Public License Version 2.0',
    classifiers=[
        'License :: OSI Approved :: Mozilla Public License 2.0 (MPL 2.0)',
        'Programming Language :: Python',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
    ],
    entry_points={
        'console_scripts': [
            'timetelld=timetell.main:main',
            'timetellcsvimporter=timetell.csvimporter:cli'
        ]
    },


    # ┏━━━━━━━━━━━━━━━━━━━━━━━━━━━┓
    # ┃ Packages and package data ┃
    # ┗━━━━━━━━━━━━━━━━━━━━━━━━━━━┛
    package_dir={'': 'src'},
    packages=find_packages('src'),
    package_data={
        'timetell': ['*.yml'],
        'timetell.handlers': ['*.yml'],
    },


    # ┏━━━━━━━━━━━━━━┓
    # ┃ Requirements ┃
    # ┗━━━━━━━━━━━━━━┛
    python_requires='~=3.6',
    setup_requires=[
        'pytest-runner'
    ],
    install_requires=[
        'aiohttp',
        'jsonschema',
        'PyYaml',

        # for postgres storage plugin
        'asyncpg', # postgres plugin

        # Recommended by aiohttp docs:
        'aiodns',    # optional asynchronous DNS client
        'uvloop',    # optional fast eventloop for asyncio
    ],
    extras_require={
        'dev': [
            'aiohttp-devtools'
        ],
        'test': [
            'pytest',
            'pytest-cov',
            'pytest-aiohttp',
        ],
    },
    # To keep PyCharm from complaining about missing requirements:
    tests_require=[
        'pytest',
        'pytest-aiohttp',
    ],
)
