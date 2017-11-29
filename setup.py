import sys

try:
    from setuptools import setup
except ImportError:
    from distutils.core import setup

# Basic dependencies
install_requires = ["pg8000", "six", "port-for"]

# Add compatibility enum class
if sys.version_info < (3, 4):
    install_requires.append("enum34")

setup(
    name='testgres',
    packages=['testgres'],
    version='1.4.1',
    description='Testing utility for PostgreSQL and its extensions',
    license='PostgreSQL',
    author='Ildar Musin',
    author_email='zildermann@gmail.com',
    url='https://github.com/postgrespro/testgres',
    keywords=['testing', 'postgresql'],
    classifiers=[],
    install_requires=install_requires)
