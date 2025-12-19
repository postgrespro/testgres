import sys

try:
    from setuptools import setup
except ImportError:
    from distutils.core import setup

# Basic dependencies
install_requires = [
    "pg8000",
    "port-for>=0.4",
    "six>=1.9.0",
    "psutil",
    "packaging",
    "testgres.os_ops>=0.0.2,<1.0.0"
]

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
    version='1.12.1',
    name='testgres',
    packages=['testgres', 'testgres.impl'],
    package_dir={"testgres": "src"},
    description='Testing utility for PostgreSQL and its extensions',
    url='https://github.com/postgrespro/testgres',
    long_description=readme,
    long_description_content_type='text/markdown',
    license='PostgreSQL',
    author='Postgres Professional',
    author_email='testgres@postgrespro.ru',
    keywords=['test', 'testing', 'postgresql'],
    install_requires=install_requires,
    classifiers=[],
)
