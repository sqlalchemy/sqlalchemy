import re

def striptags(text):
    return re.compile(r'<[^>]*>').sub('', text)

def strip_toplevel_anchors(text):
    return re.compile(r'\.html#.*-toplevel').sub('.html', text)
    
