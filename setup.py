import sys

try:
    from setuptools import setup
except ImportError:
    from distutils.core import setup

# Basic dependencies
install_requires = ["pg8000", "port-for>=0.4", "six>=1.9.0", "psutil"]

# Add compatibility enum class
if sys.version_info < (3, 4):
    install_requires.append("enum34")

# Add compatibility ipaddress module
if sys.version_info < (3, 3):
    install_requires.append("ipaddress")

setup(
    version='1.6.0',
    name='testgres',
    packages=['testgres'],
    description='Testing utility for PostgreSQL and its extensions',
    url='https://github.com/postgrespro/testgres',
    license='PostgreSQL',
    author='Ildar Musin',
    author_email='zildermann@gmail.com',
    keywords=['test', 'testing', 'postgresql'],
    install_requires=install_requires,
    classifiers=[],
)
