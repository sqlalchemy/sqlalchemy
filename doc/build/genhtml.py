#!/usr/bin/env python
import sys,re,os,shutil
import cPickle as pickle

sys.path = ['../../lib', './lib/'] + sys.path

import gen_docstrings, read_markdown, toc
from mako.lookup import TemplateLookup
from mako import exceptions, runtime
import time

files = [
    'index',
    'documentation',
    'tutorial',
    'dbengine',
    'metadata',
    'sqlconstruction',
    'datamapping',
    'unitofwork',
    'adv_datamapping',
    'types',
    'pooling',
    'plugins',
    'docstrings'
    ]

title='SQLAlchemy 0.3 Documentation'
version = '0.3.6'

root = toc.TOCElement('', 'root', '', version=version, doctitle=title)

shutil.copy('./content/index.html', './output/index.html')
shutil.copy('./content/docstrings.html', './output/docstrings.html')
shutil.copy('./content/documentation.html', './output/documentation.html')

read_markdown.parse_markdown_files(root, files)
docstrings = gen_docstrings.make_all_docs()
doc_files = gen_docstrings.create_docstring_toc(docstrings, root)

pickle.dump(docstrings, file('./output/compiled_docstrings.pickle', 'w'))
pickle.dump(root, file('./output/table_of_contents.pickle', 'w'))

template_dirs = ['./templates', './output']
output = os.path.dirname(os.getcwd())

lookup = TemplateLookup(template_dirs, output_encoding='utf-8', module_directory='./modules')

def genfile(name, outname):
    infile = name + ".html"
    outfile = file(outname, 'w')
    print infile, '->', outname
    t = lookup.get_template(infile)
    outfile.write(t.render(attributes={}))

for filename in files:
    try:
        genfile(filename, os.path.join(os.getcwd(), '../', filename + ".html"))
    except:
        print exceptions.text_error_template().render()

for filename in doc_files:
    try:
        genfile(filename, os.path.join(os.getcwd(), '../', os.path.basename(filename) + ".html"))
    except:
        print exceptions.text_error_template().render()
        



        


