#!/usr/bin/env python3

import fin_traffic_data as my_pkg
from setuptools import setup, find_packages, Command
import os


def read(fname):
    return open(os.path.join(os.path.dirname(__file__), fname)).read()


class MypyCommand(Command):

    """ Class for running the MYPY static type checker on the package """

    description = 'Runs the MYPY static type checker on ' + my_pkg.__name__
    user_options = []

    def initialize_options(self):
        ...

    def finalize_options(self):
        ...

    def run(self):
        import mypy.api
        res = mypy.api.run(['-p', my_pkg.__name__, '--ignore-missing-imports'])
        print(res[0])
        return res[1]


def get_requirements():
    with open('requirements.txt', 'r') as f:
        return f.readlines()


setup(
    name=my_pkg.__name__,
    author=my_pkg.__author__,
    author_email=my_pkg.__author_email__,
    classifiers=[
        'Development Status :: 1 - Alpha', 'Environment :: Console', 'Intended Audience :: Developers',
        'License :: Other/Proprietary License', 'Operating System :: OS Independent',
        'Programming Language :: Python :: 3 :: Only', 'Programming Language :: Python :: 3.8'
    ],
    cmdclass={
        'mypy': MypyCommand,
    },
    description="Fetching and aggregation of traffic data from Finnish roads",
    entry_points={
        'console_scripts': [
            'fin-traffic-fetch-raw-data = fin_traffic_data.scripts.fetch_raw_data:main',
            'fin-traffic-aggregate-raw-data = fin_traffic_data.scripts.aggregate_raw_data:main',
            'fin-traffic-compute-traffic-between-areas = fin_traffic_data.scripts.get_aggregated_traffic_between_areas:main',
            'fin-traffic-export-traffic-between-areas-to-csv = fin_traffic_data.scripts.export_area_data_as_csv:main',
            'fin-traffic-complete_pipeline = fin_traffic_data.scripts.complete_pipeline:main',
        ]
    },
    install_requires=get_requirements(),
    license=my_pkg.__license__,
    long_description=read('README.rst'),
    packages=find_packages(),
    package_data={
        '': ['data/*.csv',
             'data/*.json']},
    include_package_data=True,
    python_requires='>=3.6',
    test_suite='nose2.collector.collector',
    tests_require=['nose2', 'mypy'],
    url='https://www.solanpaa.fi',
    version=my_pkg.__version__,
    zip_safe=True
)
