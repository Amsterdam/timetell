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
            'ttli=timetell.importer:cli',
            'ttldl=timetell.downloader:cli'
        ]
    },


    # ┏━━━━━━━━━━━━━━━━━━━━━━━━━━━┓
    # ┃ Packages and package data ┃
    # ┗━━━━━━━━━━━━━━━━━━━━━━━━━━━┛
    package_dir={'': 'src'},
    packages=find_packages('src'),
    package_data={
        'timetell': ['*.sql'],
    },


    # ┏━━━━━━━━━━━━━━┓
    # ┃ Requirements ┃
    # ┗━━━━━━━━━━━━━━┛
    python_requires='~=3.6',
    setup_requires=[
        'pytest-runner'
    ],
    install_requires=[
        # aiohttp below is pinned because higher versions of the lib are not compatible with python3.6 
        # anymore, which is the highest installed python version on the db server on which this runs.
        # TODO: The situation described above is not an acceptable situation, and should be fixed asap.
        'aiohttp==3.8.1',  # for downloading files from objectstore. 
        'asyncpg',  # async postgres driver
        'uvloop<0.15.0',  # optional fast eventloop for asyncio
    ],
    extras_require={
        'test': [
            'pytest',
            'pytest-cov',
        ],
    },
    # To keep PyCharm from complaining about missing requirements:
    tests_require=[
        'pytest',
    ],
)
