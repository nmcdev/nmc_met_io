# _*_ coding: utf-8 _*_

from os import path
from setuptools import find_packages, setup
from codecs import open


name = 'nmc_met_io'
author = __import__(name).__author__
version = __import__(name).__version__

here = path.abspath(path.dirname(__file__))

# Get the long description from the README file
with open(path.join(here, 'README.md'), encoding='utf-8') as f:
    long_description = f.read()

setup(
    name=name,
    version=version,

    description=("Collection of tools for I/O or accessing meteorological data."),
    long_description=long_description,
    long_description_content_type='text/markdown',
    url='https://github.com/nmcdev/nmc_met_io',

    # author
    author=author,
    author_email='kan.dai@foxmail.com',

    # LICENSE
    license='GPL3',

    classifiers=[
      'Development Status :: 4 - Beta',
      'Intended Audience :: Developers',
      'Intended Audience :: Science/Research',
      'License :: OSI Approved :: GNU General Public License v3 (GPLv3)',
      'Topic :: Scientific/Engineering',
      'Topic :: Scientific/Engineering :: Atmospheric Science',
      'Programming Language :: Python :: 3',
      'Programming Language :: Python :: 3.6',
      'Programming Language :: Python :: 3.7',
      'Programming Language :: Python :: 3.8',
      'Operating System :: POSIX :: Linux',
      'Operating System :: MacOS :: MacOS X',
      'Operating System :: Microsoft :: Windows'],

    python_requires='>=3.6',
    zip_safe = False,
    platforms = ["all"],

    packages=find_packages(exclude=[
      'documents', 'docs', 'examples', 'notebooks', 'tests', 'build', 'dist']),
    include_package_data=True,
    exclude_package_data={'': ['.gitignore']},

    install_requires=[
      'numpy>=1.17.0',
      'scipy>=1.4.0',
      'pandas>=1.0.0',
      'xarray>=0.16.0',
      'protobuf>=3.12.0',
      'urllib3>=1.25.9',
      'tqdm>=4.47.0',
      'python-dateutil>=2.8.1']
)

# development mode (DOS command):
#     python setup.py develop
#     python setup.py develop --uninstall

# build mode：
#     python setup.py build --build-base=D:/test/python/build

# distribution mode:
#     python setup.py sdist build              # create source tar.gz file in /dist
#     twine upload --skip-existing dist/*      # upload package to pypi
