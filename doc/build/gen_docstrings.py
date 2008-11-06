from toc import TOCElement
import docstring
import re

from sqlalchemy import schema, types, engine, sql, pool, orm, exceptions, databases, interfaces
from sqlalchemy.sql import compiler, expression
from sqlalchemy.engine import default, strategies, threadlocal, url
from sqlalchemy.orm import shard
from sqlalchemy.ext import orderinglist, associationproxy, sqlsoup, declarative, serializer

def make_doc(obj, classes=None, functions=None, **kwargs):
    """generate a docstring.ObjectDoc structure for an individual module, list of classes, and list of functions."""
    obj = docstring.ObjectDoc(obj, classes=classes, functions=functions, **kwargs)
    return (obj.name, obj)

def make_all_docs():
    """generate a docstring.AbstractDoc structure."""
    print "generating docstrings"
    objects = [
        make_doc(obj=engine),
        make_doc(obj=default),
        make_doc(obj=strategies),
        make_doc(obj=threadlocal),
        make_doc(obj=url),
        make_doc(obj=exceptions),
        make_doc(obj=interfaces),
        make_doc(obj=pool),
        make_doc(obj=schema),
        #make_doc(obj=sql,include_all_classes=True),
        make_doc(obj=compiler),
        make_doc(obj=expression,include_all_classes=True),
        make_doc(obj=types),
        make_doc(obj=orm),
        make_doc(obj=orm.attributes),
        make_doc(obj=orm.collections, classes=[orm.collections.collection,
                                               orm.collections.MappedCollection,
                                               orm.collections.CollectionAdapter]),
        make_doc(obj=orm.interfaces),
        make_doc(obj=orm.mapperlib, classes=[orm.mapperlib.Mapper]),
        make_doc(obj=orm.properties),
        make_doc(obj=orm.query, classes=[orm.query.Query]),
        make_doc(obj=orm.session, classes=[orm.session.Session, orm.session.SessionExtension]),
        make_doc(obj=orm.shard),
        make_doc(obj=declarative),
        make_doc(obj=associationproxy, classes=[associationproxy.AssociationProxy]),
        make_doc(obj=orderinglist, classes=[orderinglist.OrderingList]),
        make_doc(obj=serializer),
        make_doc(obj=sqlsoup),
    ] + [make_doc(getattr(__import__('sqlalchemy.databases.%s' % m).databases, m)) for m in databases.__all__]
    return objects
    
def create_docstring_toc(data, root):
    """given a docstring.AbstractDoc structure, create new TOCElement nodes corresponding
    to the elements and cross-reference them back to the doc structure."""
    root = TOCElement("docstrings", name="docstrings", description="API Documentation", parent=root, requires_paged=True)
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
