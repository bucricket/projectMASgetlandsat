#!/usr/bin/env python

from __future__ import print_function
import os



# set project base directory structure
base = os.getcwd()
    
try:
    from setuptools import setup
    setup_kwargs = {'entry_points': {'console_scripts':['getlandsatdata=getlandsatdata.getlandsatdata:main']}}
except ImportError:
    from distutils.core import setup
    setup_kwargs = {'scripts': ['bin/getlandsatdata']}
    
from getlandsatdata import __version__




setup(
    name="getlandsatdata",
    version=__version__,
    description="get Landsat data",
    author="Mitchell Schull",
    author_email="mitch.schull@noaa.gov",
    url="https://github.com/bucricket/projectMASgetmodis.git",
#    packages= ['getlandsatdata'],
    py_modules=['getlandsatdata.getlandsatdata'],
    platforms='Posix; MacOS X; Windows',
    license='BSD 3-Clause',
    classifiers=[
        'Development Status :: 2 - Pre-Alpha',
        'Intended Audience :: Developers',
        'Intended Audience :: Science/Research',
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python :: 2',
        # Uses dictionary comprehensions ==> 2.7 only
        'Programming Language :: Python :: 2.7',
        'Topic :: Scientific/Engineering :: GIS',
    ],  
    **setup_kwargs
)

