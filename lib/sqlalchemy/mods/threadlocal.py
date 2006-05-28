from sqlalchemy import util, engine, mapper
from sqlalchemy.ext.sessioncontext import SessionContext
import sqlalchemy.ext.assignmapper as assignmapper
from sqlalchemy.orm.mapper import global_extensions
from sqlalchemy.orm.session import Session
import sqlalchemy
import sys, types

"""this plugin installs thread-local behavior at the Engine and Session level.

The default Engine strategy will be "threadlocal", producing TLocalEngine instances for create_engine by default.
With this engine, connect() method will return the same connection on the same thread, if it is already checked out
from the pool.  this greatly helps functions that call multiple statements to be able to easily use just one connection
without explicit "close" statements on result handles.

on the Session side, module-level methods will be installed within the objectstore module, such as flush(), delete(), etc.
which call this method on the thread-local session.

Note: this mod creates a global, thread-local session context named sqlalchemy.objectstore. All mappers created
while this mod is installed will reference this global context when creating new mapped object instances.
"""

__all__ = ['Objectstore', 'assign_mapper']

class Objectstore(SessionContext):
    def __getattr__(self, key):
        return getattr(self.current, key)
    def get_session(self):
        return self.current

def assign_mapper(class_, *args, **kwargs):
    assignmapper.assign_mapper(objectstore, class_, *args, **kwargs)

objectstore = Objectstore(Session)
def install_plugin():
    sqlalchemy.objectstore = objectstore
    global_extensions.append(objectstore.mapper_extension)
    engine.default_strategy = 'threadlocal'
    sqlalchemy.assign_mapper = assign_mapper

def uninstall_plugin():
    engine.default_strategy = 'plain'
    global_extensions.remove(objectstore.mapper_extension)

install_plugin()
