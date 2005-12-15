#!/usr/bin/env python
import sys,re,os

component_root = [
    {'components': './components'},
    {'content' : './content'}
]
doccomp = ['document_base.myt']
output = os.path.dirname(os.getcwd())

sys.path = ['../../lib', './lib/'] + sys.path

import myghty.http.HTTPServerHandler as HTTPServerHandler

httpd = HTTPServerHandler.HTTPServer(
    port = 8080,
    
    handlers = [
        {'.*(?:\.myt|/$)' : HTTPServerHandler.HSHandler(path_translate=[(r'^/$', r'/index.myt')], data_dir = './cache', component_root = component_root)},
    ],

    docroot = [{'.*' : '../'}],
    
)       
        
httpd.serve_forever()