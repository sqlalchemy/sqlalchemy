import pkgutil

import sqlalchemy

list(pkgutil.walk_packages(sqlalchemy.__path__, sqlalchemy.__name__ + "."))
