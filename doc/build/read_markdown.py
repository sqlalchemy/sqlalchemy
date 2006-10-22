"""loads Markdown files, converts each one to HTML and parses the HTML into an ElementTree structure.
The collection of ElementTrees are further parsed to generate a table of contents structure, and are  
 manipulated to replace various markdown-generated HTML with specific Myghty tags before being written
 to Myghty templates, which then re-access the table of contents structure at runtime.

Much thanks to Alexey Shamrin, who came up with the original idea and did all the heavy Markdown/Elementtree 
lifting for this module."""
import sys, re, os
from toc import TOCElement

try:
    import elementtree.ElementTree as et
except:
    raise "This module requires ElementTree to run (http://effbot.org/zone/element-index.htm)"

import markdown

def dump_tree(elem, stream):
    if elem.tag.startswith('MYGHTY:'):
        dump_myghty_tag(elem, stream)
    else:
        if len(elem.attrib):
            stream.write("<%s %s>" % (elem.tag, " ".join(["%s=%s" % (key, repr(val)) for key, val in elem.attrib.iteritems()])))
        else:
            stream.write("<%s>" % elem.tag)
        if elem.text:
            stream.write(elem.text)
        for child in elem:
            dump_tree(child, stream)
            if child.tail:
                stream.write(child.tail)
        stream.write("</%s>" % elem.tag)

def dump_myghty_tag(elem, stream):
    tag = elem.tag[7:]
    params = ', '.join(['%s=%s' % i for i in elem.items()])
    pipe = ''
    if elem.text or len(elem):
        pipe = '|'
    comma = ''
    if params:
        comma = ', '
    stream.write('<&%s%s%s%s&>' % (pipe, tag, comma, params))
    if pipe:
        if elem.text:
            stream.write(elem.text)
        for n in elem:
            dump_tree(n, stream)
            if n.tail:
                stream.write(n.tail)
        stream.write("</&>")

def create_toc(filename, tree, tocroot):
    title = [None]
    current = [tocroot]
    level = [0]
    def process(tree):
        while True:
            i = find_header_index(tree)
            if i is None:
                return
            node = tree[i]
            taglevel = int(node.tag[1])
            start, end = i, end_of_header(tree, taglevel, i+1)
            content = tree[start+1:end]
            description = node.text.strip()
            if title[0] is None:
                title[0] = description
            name = node.get('name')
            if name is None:
                name = description.split()[0].lower()
            
            taglevel = node.tag[1]
            if taglevel > level[0]:
                current[0] = TOCElement(filename, name, description, current[0])
            elif taglevel == level[0]:
                current[0] = TOCElement(filename, name, description, current[0].parent)
            else:
                current[0] = TOCElement(filename, name, description, current[0].parent.parent)

            level[0] = taglevel

            tag = et.Element("MYGHTY:formatting.myt:section", path=literal(current[0].path), toc="toc")
            tag.text = (node.tail or "") + '\n'
            tag.tail = '\n'
            tag[:] = content
            tree[start:end] = [tag]

            process(tag)

    process(tree)
    return (title[0], tocroot.get_by_file(filename))

def literal(s):
    return '"%s"' % s
    
def index(parent, item):
    for n, i in enumerate(parent):
        if i is item:
            return n

def find_header_index(tree):
    for i, node in enumerate(tree):
        if is_header(node):
            return i

def is_header(node):
    t = node.tag
    return (isinstance(t, str) and len(t) == 2 and t[0] == 'h' 
            and t[1] in '123456789')

def end_of_header(tree, level, start):
    for i, node in enumerate(tree[start:]):
        if is_header(node) and int(node.tag[1]) <= level:
            return start + i
    return len(tree)

def process_rel_href(tree):
    parent = get_parent_map(tree)
    for a in tree.findall('.//a'):
        m = re.match(r'(bold)?rel\:(.+)', a.get('href'))
        if m:
            (bold, path) = m.group(1,2)
            text = a.text
            if text == path:
                tag = et.Element("MYGHTY:nav.myt:toclink", path=literal(path), toc="toc", extension="extension")
            else:
                tag = et.Element("MYGHTY:nav.myt:toclink", path=literal(path), description=literal(text), toc="toc", extension="extension")
            a_parent = parent[a]
            if bold:
                bold = et.Element('strong')
                bold.tail = a.tail
                bold.append(tag)
                a_parent[index(a_parent, a)] = bold
            else:
                tag.tail = a.tail
                a_parent[index(a_parent, a)] = tag

def replace_pre_with_myt(tree):
    def splice_code_tag(pre, text, type=None, title=None):
        doctest_directives = re.compile(r'#\s*doctest:\s*[+-]\w+(,[+-]\w+)*\s*$', re.M)
        text = re.sub(doctest_directives, '', text)
        # process '>>>' to have quotes around it, to work with the myghty python
        # syntax highlighter which uses the tokenize module
        text = re.sub(r'>>> ', r'">>>" ', text)

        # indent two spaces.  among other things, this helps comment lines "#  " from being 
        # consumed as Myghty comments.
        text = re.compile(r'^(?!<&)', re.M).sub('  ', text)

        sqlre = re.compile(r'{sql}(.*?)((?:SELECT|INSERT|DELETE|UPDATE|CREATE|DROP|PRAGMA|DESCRIBE).*?)\n\s*(\n|$)', re.S)
        if sqlre.search(text) is not None:
            use_sliders = False
        else:
            use_sliders = True
        
        text = sqlre.sub(r"<&formatting.myt:poplink&>\1\n<&|formatting.myt:codepopper, link='sql'&>\2</&>\n\n", text)

        sqlre2 = re.compile(r'{opensql}(.*?)((?:SELECT|INSERT|DELETE|UPDATE|CREATE|DROP).*?)\n\s*(\n|$)', re.S)
        text = sqlre2.sub(r"<&|formatting.myt:poppedcode &>\1\n\2</&>\n\n", text)

        opts = {}
        if type == 'python':
            opts['syntaxtype'] = literal('python')
        else:
            opts['syntaxtype'] = None

        if title is not None:
            opts['title'] = literal(title)
    
        if use_sliders:
            opts['use_sliders'] = True
    
        tag = et.Element("MYGHTY:formatting.myt:code", **opts)
        tag.text = text

        pre_parent = parents[pre]
        tag.tail = pre.tail
        pre_parent[reverse_parent(pre_parent, pre)] = tag

    parents = get_parent_map(tree)

    for precode in tree.findall('.//pre/code'):
        m = re.match(r'\{(python|code)(?: title="(.*?)"){0,1}\}', precode.text.lstrip())
        if m:
            code = m.group(1)
            title = m.group(2)
            text = precode.text.lstrip()
            text = re.sub(r'{(python|code).*?}(\n\s*)?', '', text)
            splice_code_tag(parents[precode], text, type=code, title=title)
        elif precode.text.lstrip().startswith('>>> '):
            splice_code_tag(parents[precode], precode.text)

def reverse_parent(parent, item):
    for n, i in enumerate(parent):
        if i is item:
            return n

def get_parent_map(tree):
    return dict([(c, p) for p in tree.getiterator() for c in p])

def header(toc, title, filename):
    return """#encoding: utf-8
<%%flags>
    inherit='content_layout.myt'
</%%flags>
<%%args>
    toc
    extension
</%%args>
<%%attr>
    title='%s - %s'
    filename = '%s'
</%%attr>
<%%doc>This file is generated.  Edit the .txt files instead of this one.</%%doc>
""" % (toc.root.doctitle, title, filename)
  
class utf8stream(object):
    def __init__(self, stream):
        self.stream = stream
    def write(self, str):
        self.stream.write(str.encode('utf8'))
        
def parse_markdown_files(toc, files):
    for inname in files:
        infile = 'content/%s.txt' % inname
        if not os.access(infile, os.F_OK):
            continue
        html = markdown.markdown(file(infile).read())
        tree = et.fromstring("<html>" + html + "</html>")
        (title, toc_element) = create_toc(inname, tree, toc)
        replace_pre_with_myt(tree)
        process_rel_href(tree)
        outname = 'output/%s.myt' % inname
        print infile, '->', outname
        outfile = utf8stream(file(outname, 'w'))
        outfile.write(header(toc, title, inname))
        dump_tree(tree, outfile)
    
    
