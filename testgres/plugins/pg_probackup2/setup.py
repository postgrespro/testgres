try:
    from setuptools import setup
except ImportError:
    from distutils.core import setup

setup(
    version='0.0.3',
    name='testgres_pg_probackup2',
    packages=['pg_probackup2', 'pg_probackup2.storage'],
    description='Plugin for testgres that manages pg_probackup2',
    url='https://github.com/postgrespro/testgres',
    long_description_content_type='text/markdown',
    license='PostgreSQL',
    author='Postgres Professional',
    author_email='testgres@postgrespro.ru',
    keywords=['pg_probackup', 'testing', 'testgres'],
    install_requires=['testgres>=1.9.2']
)
