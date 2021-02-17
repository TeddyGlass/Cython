from setuptools import setup
from Cython.Build import cythonize

setup(
    ext_modules = cythonize(["helloworld_1.pyx", "helloworld_2.pyx"])
)