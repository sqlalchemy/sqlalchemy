#!/usr/bin/env python
import sys,re,os

"""starts an HTTP server which will serve generated .myt files from the ./components and 
./output directories."""


component_root = [
    {'components': './components'},
    {'content' : './output'}
]
doccomp = ['document_base.myt']
output = os.path.dirname(os.getcwd())

sys.path = ['./lib/'] + sys.path

import myghty.http.HTTPServerHandler as HTTPServerHandler

port = 8080
httpd = HTTPServerHandler.HTTPServer(
    port = port,
    handlers = [
        {'.*(?:\.myt|/$)' : HTTPServerHandler.HSHandler(path_translate=[(r'^/$', r'/index.myt')], data_dir = './cache', component_root = component_root, output_encoding='utf-8')},
    ],

    docroot = [{'.*' : '../'}],
    
)       

print "Listening on %d" % port        
httpd.serve_forever()
