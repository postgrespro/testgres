from distutils.core import setup
setup(
  name = 'testgres',
  packages = ['testgres'],
  version = '0.1.5',
  description = 'Testing utility for postgresql and it''s extensions',
  author = 'Ildar Musin',
  author_email = 'zildermann@gmail.com',
  url = 'https://github.com/postgrespro/testgres',
  #download_url = 'https://github.com/postgrespro/testgres/tarball/0.1.1',
  keywords = ['testing', 'postgresql'],
  classifiers = [],
  install_requires = ["pg8000"]
)
