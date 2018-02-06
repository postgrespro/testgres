import sys

try:
    from setuptools import setup
except ImportError:
    from distutils.core import setup

# Basic dependencies
install_requires = ["pg8000", "port-for", "six"]

# Add compatibility enum class
if sys.version_info < (3, 4):
    install_requires.append("enum34")

# Add compatibility ipaddress module
if sys.version_info < (3, 3):
    install_requires.append("ipaddress")

setup(
    name='testgres',
    packages=['testgres'],
    version='1.5.0',
    description='Testing utility for PostgreSQL and its extensions',
    license='PostgreSQL',
    author='Ildar Musin',
    author_email='zildermann@gmail.com',
    url='https://github.com/postgrespro/testgres',
    keywords=['testing', 'postgresql'],
    classifiers=[],
    install_requires=install_requires)
