#!/usr/bin/env python
import sys,re,os

component_root = [
    {'components': './components'},
    {'content' : './content'}
]
doccomp = ['document_base.myt']
output = os.path.dirname(os.getcwd())

sys.path = ['../../lib', './lib/'] + sys.path

import documentgen

documentgen.genall(doccomp, component_root, output)



