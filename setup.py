#!/usr/bin/env python



from distutils.core import setup
import sys

sys.path.append('./lib')

setup(name = "SQLAlchemy",
    version = "0.91",
    description = "Database Abstraction Library",
    author = "Mike Bayer",
    author_email = "mike_mp@zzzcomputing.com",
    url = "http://sqlalchemy.sourceforge.net",
    packages = ["sqlalchemy", "sqlalchemy.databases"],
    package_dir = {'' : 'lib'},
    license = "GNU Lesser General Public License"
    )


