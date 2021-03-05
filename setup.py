from setuptools import (
    Extension,
    find_packages,
    setup,
)

import numpy

from Cython.Build import cythonize
from Cython.Distutils import build_ext 

ext_modules = [
    Extension('fsharadar.daily.cython_read_int64', ['fsharadar/daily/cython_read_int64.pyx'], include_dirs=[numpy.get_include()]),
    ]

setup(
    name='fsharadar',
    cmdclass = {'build_ext': build_ext },
    packages=find_packages(include=['fsharadar', 'fsharadar.*']),
    ext_modules=ext_modules,
    include_dirs=[numpy.get_include()],
    zip_safe=False,
)
