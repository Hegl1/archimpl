#!/usr/bin/env python
"""The setup script."""

from glob import glob
from os.path import basename
from os.path import splitext

from setuptools import find_packages
from setuptools import setup

with open('README.md') as readme_file:
    readme = readme_file.read()

requirements = [
    'click>=7.0',
]

setup_requirements = [
    'pytest-runner',
]

test_requirements = [
    'pytest>=3',
]

setup(
    author='DBIS',
    author_email='maximilian.mayerl@uibk.ac.at',
    python_requires='>=3.6',
    classifiers=[
        'Development Status :: 2 - Pre-Alpha',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: BSD License',
        'Natural Language :: English',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3.8',
        'Programming Language :: Python :: 3.9',
    ],
    description=' '.join((
        'MOSAIC is a database implementation in python used to teach the',
        'implementation of databases.',
    )),
    entry_points={
        'console_scripts': ['mosaic=mosaic.cli:main'],
    },
    install_requires=requirements,
    license='BSD license',
    long_description=readme,
    include_package_data=True,
    keywords='mosaic',
    name='mosaic',
    packages=find_packages('src'),
    package_dir={'': 'src'},
    py_modules=[splitext(basename(path))[0] for path in glob('src/*.py')],
    setup_requires=setup_requirements,
    test_suite='tests',
    tests_require=test_requirements,
    url='https://dbis.uibk.ac.at',
    version='0.1.0',
    zip_safe=False,
)
