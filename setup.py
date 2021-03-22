from setuptools import (
    Extension,
    find_packages,
    setup,
)

import numpy

from Cython.Build import cythonize
from Cython.Distutils import build_ext 

ext_modules = [
    Extension('fsharadar.cython_read_float64', ['fsharadar/cython_read_float64.pyx'], include_dirs=[numpy.get_include()]),
    ]

setup(
    name='fsharadar',
    cmdclass = {'build_ext': build_ext },
    packages=find_packages(include=['fsharadar', 'fsharadar.*']),
    ext_modules=ext_modules,
    include_dirs=[numpy.get_include()],
    zip_safe=False,
)
