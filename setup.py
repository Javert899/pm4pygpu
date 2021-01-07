from os.path import dirname, join

import pm4pygpu
from setuptools import setup


def read_file(filename):
    with open(join(dirname(__file__), filename)) as f:
        return f.read()


setup(
    name="pm4pygpu",
    version=pm4pygpu.__version__,
    description=pm4pygpu.__doc__.strip(),
    long_description=read_file('README.md'),
    author=pm4pygpu.__author__,
    author_email=pm4pygpu.__author_email__,
    py_modules=[pm4pygpu.__name__],
    include_package_data=True,
    packages=["pm4pygpu"],
    url='http://www.pm4py.org',
    license='GPL-3.0',
    install_requires=[
        "pm4py"
    ]
)
