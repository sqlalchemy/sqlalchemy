#!/usr/bin/env python
import sys,re,os

print "Running txt2myt.py..."
execfile("txt2myt.py")

component_root = [
    {'components': './components'},
    {'content' : './content'}
]
doccomp = ['document_base.myt']
output = os.path.dirname(os.getcwd())

sys.path = ['../../lib', './lib/'] + sys.path

import documentgen

documentgen.genall(doccomp, component_root, output)



