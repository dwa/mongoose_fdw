###
### Author: David Wallin
### Time-stamp: <2015-03-01 17:29:15 dwa>

from setuptools import setup

if __name__ == '__main__':
    setup(name='mongoose_fdw',
          author='David Wallin',
          author_email='dwa@havanaclub.org',
          description='Alternative Postgres fdw for MongoDB',
          url='http://github.com/dwa/mongoose_fdw',
          version='0.0.1',
          install_requires=['pymongo'],
          packages=['mongoose_fdw'],
          classifier=['Private :: Do Not Upload'])

## Local Variables: ***
## mode:python ***
## coding: utf-8 ***
## End: ***
