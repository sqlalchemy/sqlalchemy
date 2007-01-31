from toc import TOCElement
import docstring

import sqlalchemy.schema as schema
import sqlalchemy.types as types
import sqlalchemy.engine as engine
import sqlalchemy.engine.strategies as strategies
import sqlalchemy.sql as sql
import sqlalchemy.pool as pool
import sqlalchemy.orm as orm
import sqlalchemy.exceptions as exceptions
import sqlalchemy.ext.proxy as proxy
import sqlalchemy.ext.sessioncontext as sessioncontext
import sqlalchemy.mods.threadlocal as threadlocal
import sqlalchemy.ext.selectresults as selectresults

def make_doc(obj, classes=None, functions=None):
    """generate a docstring.ObjectDoc structure for an individual module, list of classes, and list of functions."""
    return docstring.ObjectDoc(obj, classes=classes, functions=functions)

def make_all_docs():
    """generate a docstring.AbstractDoc structure."""
    objects = [
        make_doc(obj=sql),
        make_doc(obj=schema),
        make_doc(obj=types),
        make_doc(obj=engine),
        make_doc(obj=engine.url),
        make_doc(obj=orm, classes=[orm.MapperExtension]),
        make_doc(obj=orm.mapperlib, classes=[orm.mapperlib.Mapper]),
        make_doc(obj=orm.query, classes=[orm.query.Query, orm.query.QueryContext, orm.query.SelectionContext]),
        make_doc(obj=orm.session, classes=[orm.session.Session, orm.session.SessionTransaction]),
        make_doc(obj=pool),
        make_doc(obj=sessioncontext),
        make_doc(obj=threadlocal),
        make_doc(obj=selectresults),
        make_doc(obj=exceptions),
        make_doc(obj=proxy),
    ]
    return objects
    
def create_docstring_toc(data, root):
    """given a docstring.AbstractDoc structure, create new TOCElement nodes corresponding
    to the elements and cross-reference them back to the doc structure."""
    root = TOCElement("docstrings", name="docstrings", description="Generated Documentation", parent=root)
    def create_obj_toc(obj, toc):
        if obj.isclass:
            s = []
            for elem in obj.inherits:
                if isinstance(elem, docstring.ObjectDoc):
                    s.append(elem.name)
                else:
                    s.append(str(elem))
            description = "class " + obj.classname + "(%s)" % (','.join(s))
        else:
            description = obj.description

        toc = TOCElement("docstrings", obj.name, description, parent=toc)
        obj.toc_path = toc.path

        if not obj.isclass and obj.functions:
            functoc = TOCElement("docstrings", name="modfunc", description="Module Functions", parent=toc)
            obj.mod_path = functoc.path
            
        if obj.classes:
            for class_ in obj.classes:
                create_obj_toc(class_, toc)
                
    for obj in data:
        create_obj_toc(obj, root)
    return data

