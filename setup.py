from distutils.core import setup
setup(
    name='testgres',
    packages=['testgres'],
    version='0.4.0',
    description='Testing utility for postgresql and its extensions',
    license='PostgreSQL',
    author='Ildar Musin',
    author_email='zildermann@gmail.com',
    url='https://github.com/postgrespro/testgres',
    keywords=['testing', 'postgresql'],
    classifiers=[],
    install_requires=["pg8000", "six", "port-for"])
