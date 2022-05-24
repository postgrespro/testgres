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

# Get contents of README file
with open('README.md', 'r') as f:
    readme = f.read()

setup(
    version='1.8.5',
    name='testgres',
    packages=['testgres'],
    description='Testing utility for PostgreSQL and its extensions',
    url='https://github.com/postgrespro/testgres',
    long_description=readme,
    long_description_content_type='text/markdown',
    license='PostgreSQL',
    author='Ildar Musin',
    author_email='zildermann@gmail.com',
    keywords=['test', 'testing', 'postgresql'],
    install_requires=install_requires,
    classifiers=[],
)
