from toc import TOCElement
import docstring
import re

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
import sqlalchemy.databases as databases

def make_doc(obj, classes=None, functions=None):
    """generate a docstring.ObjectDoc structure for an individual module, list of classes, and list of functions."""
    obj = docstring.ObjectDoc(obj, classes=classes, functions=functions)
    return (obj.name, obj)

def make_all_docs():
    """generate a docstring.AbstractDoc structure."""
    print "generating docstrings"
    objects = [
        make_doc(obj=sql),
        make_doc(obj=schema),
        make_doc(obj=types),
        make_doc(obj=engine),
        make_doc(obj=engine.url),
        make_doc(obj=orm),
        make_doc(obj=orm.mapperlib, classes=[orm.mapperlib.MapperExtension, orm.mapperlib.Mapper]),
        make_doc(obj=orm.query, classes=[orm.query.Query, orm.query.QueryContext, orm.query.SelectionContext]),
        make_doc(obj=orm.session, classes=[orm.session.Session, orm.session.SessionTransaction]),
        make_doc(obj=pool),
        make_doc(obj=sessioncontext),
        make_doc(obj=threadlocal),
        make_doc(obj=selectresults),
        make_doc(obj=exceptions),
        make_doc(obj=proxy),
    ] + [make_doc(getattr(__import__('sqlalchemy.databases.%s' % m).databases, m)) for m in databases.__all__]
    return objects
    
def create_docstring_toc(data, root):
    """given a docstring.AbstractDoc structure, create new TOCElement nodes corresponding
    to the elements and cross-reference them back to the doc structure."""
    root = TOCElement("docstrings", name="docstrings", description="Generated Documentation", parent=root, requires_paged=True)
    files = []
    def create_obj_toc(obj, toc):
        if obj.isclass:
            s = []
            for elem in obj.inherits:
                if isinstance(elem, docstring.ObjectDoc):
                    s.append(elem.name)
                else:
                    s.append(str(elem))
            description = "class " + obj.classname + "(%s)" % (','.join(s))
            filename = toc.filename
        else:
            description = obj.description
            filename = re.sub(r'\W', '_', obj.name)
            
        toc = TOCElement(filename, obj.name, description, parent=toc, requires_paged=True)
        obj.toc_path = toc.path
        if not obj.isclass:
            create_module_file(obj, toc)
            files.append(filename)
            
        if not obj.isclass and obj.functions:
            functoc = TOCElement(toc.filename, name="modfunc", description="Module Functions", parent=toc)
            obj.mod_path = functoc.path
            for func in obj.functions:
                t = TOCElement(toc.filename, name=func.name, description=func.name + "()", parent=functoc)
                func.toc_path = t.path
        #elif obj.functions:
        #    for func in obj.functions:
        #        t = TOCElement(toc.filename, name=func.name, description=func.name, parent=toc)
        #        func.toc_path = t.path
            
        if obj.classes:
            for class_ in obj.classes:
                create_obj_toc(class_, toc)
                
    for key, obj in data:
        create_obj_toc(obj, root)
    return files

def create_module_file(obj, toc):
    outname = 'output/%s.html' % toc.filename
    print "->", outname
    header = """# -*- coding: utf-8 -*-
    <%%inherit file="module.html"/>
    <%%def name="title()">%s - %s</%%def>
    ## This file is generated.  Edit the .txt files instead of this one.
    <%%!
        filename = '%s'
        docstring = '%s'
    %%>
    """ % (toc.root.doctitle, obj.description, toc.filename, obj.name)
    file(outname, 'w').write(header)
    return outname