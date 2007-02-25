from sqlalchemy.ext.selectresults import *
from sqlalchemy.orm.mapper import global_extensions

def install_plugin():
    global_extensions.append(SelectResultsExt)

install_plugin()
