#!/usr/bin/env python3

import fin_traffic_data as my_pkg
from setuptools import setup, find_packages, Command
import os
import subprocess
import re
import sys


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
        'Development Status :: 1 - Alpha', 'Environment :: Console',
        'Intended Audience :: Developers',
        'License :: Other/Proprietary License',
        'Operating System :: OS Independent',
        'Programming Language :: Python :: 3 :: Only',
        'Programming Language :: Python :: 3.8'
    ],
    cmdclass={
        'mypy': MypyCommand,
    },
    data_files=[],
    description="Fetching and aggregating traffic data from Finnish roads",
    install_requires=get_requirements(),
    license=my_pkg.__license__,
    long_description=read('README.rst'),
    packages=find_packages(),
    python_requires='>=3.7',
    test_suite='nose2.collector.collector',
    tests_require=['nose2', 'mypy'],
    url='https://www.solanpaa.fi',
    version=my_pkg.__version__,
    zip_safe=True)
