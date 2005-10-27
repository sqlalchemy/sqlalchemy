from ez_setup import use_setuptools
use_setuptools()
from setuptools import setup, find_packages

setup(name = "SQLAlchemy",
    version = "0.91alpha",
    description = "Database Abstraction Library",
    author = "Mike Bayer",
    author_email = "mike_mp@zzzcomputing.com",
    url = "http://sqlalchemy.sourceforge.net",
    packages = find_packages('lib'),
    package_dir = {'':'lib'},
    license = "GNU Lesser General Public License",
    long_description = """A Python SQL toolkit and object relational mapper for application developers.""",
    classifiers = [
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: GNU Library or Lesser General Public License (LGPL)",
        "Programming Language :: Python",
        "Topic :: Database :: Front-Ends",
    ]
    )




