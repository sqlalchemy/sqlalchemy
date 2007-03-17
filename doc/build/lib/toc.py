"""
defines a pickleable, recursive "table of contents" datastructure.

TOCElements define a name, a description, and also a uniquely-identifying "path" which is
used to generate hyperlinks between document sections.
"""
import time, re

toc_by_file = {}
toc_by_path = {}
filenames = []

class TOCElement(object):
    def __init__(self, filename, name, description, parent=None, version=None, last_updated=None, doctitle=None, requires_paged=False, **kwargs):
        self.filename = filename
        self.name = re.sub(r'[<>&;%]', '', name)
        self.description = description
        self.parent = parent
        self.content = None
        self.filenames = filenames
        self.toc_by_path = toc_by_path
        self.toc_by_file = toc_by_file
        self.last_updated = time.time()
        self.version = version
        self.doctitle = doctitle
        self.requires_paged = requires_paged
        (self.path, self.depth) = self._create_path()
        #print "NEW TOC:", self.path
        for key, value in kwargs.iteritems():
            setattr(self, key, value)

        toc_by_path[self.path] = self
            
        self.is_top = (self.parent is not None and self.parent.filename != self.filename) or self.parent is None
        if self.is_top:
            toc_by_file[self.filename] = self
            if self.filename:
                filenames.append(self.filename)
                
        self.root = self.parent and self.parent.root or self

        self.content = None
        self.previous = None
        self.next = None
        self.children = []
        if parent:
            if len(parent.children):
                self.previous = parent.children[-1]
                parent.children[-1].next = self
            parent.children.append(self)
            if parent is not parent.root:
                self.up = parent
            else:
                self.up = None
                
    def get_page_root(self):
        return self.toc_by_file[self.filename]
        
    def get_by_path(self, path):
        return self.toc_by_path.get(path)

    def get_by_file(self, filename):
        return self.toc_by_file[filename]

    def get_link(self, extension='html', anchor=True, usefilename=True):
        if usefilename or self.requires_paged:
            if anchor:
                return "%s.%s#%s" % (self.filename, extension, self.path) 
            else:
                return "%s.%s" % (self.filename, extension)
        else:
            return "#%s" % (self.path) 


    def _create_path(self):
        elem = self
        tokens = []
        depth = 0
        while elem.parent is not None:
            tokens.insert(0, elem.name)
            elem = elem.parent
            depth +=1
        return ('_'.join(tokens), depth)
