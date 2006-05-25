"""txt2myt.py -- translate all .txt files in a `content` directory to
.myt template, assuming .txt conform to Markdown syntax.
"""

import sys
import string
import re

try:
    import elementtree.ElementTree as et
except:
    raise "This module requires ElementTree to run (http://effbot.org/zone/element-index.htm)"

sys.path.insert(0, './lib')
import markdown

class MyElementTree(et.ElementTree):
    def _write(self, file, node, encoding, namespaces):
        """With support for myghty 'tags'"""
        if node.tag is MyghtyTag:
            params = ', '.join(['%s="%s"' % i for i in node.items()])
            pipe = ''
            if node.text or len(node):
                pipe = '|'
            comma = ''
            if params:
                comma = ', '
            file.write('<&%s%s%s%s&>' % (pipe, node.name, comma, params))
            if pipe:
                if node.text:
                    file.write(node.text)
                for n in node:
                    self._write(file, n, encoding, namespaces)
                file.write("</&>")
            if node.tail:
                file.write(node.tail)
        else:
            et.ElementTree._write(self, file, node, encoding, namespaces)

# The same as et.dump, but using MyElementTree
def dump(elem):
    # debugging
    if not isinstance(elem, et.ElementTree):
        elem = MyElementTree(elem)
    elem.write(sys.stdout)
    tail = elem.getroot().tail
    if not tail or tail[-1] != "\n":
        sys.stdout.write("\n")

# The same as et.tostring, but using MyElementTree
def tostring(element, encoding=None):
    class dummy:
        pass
    data = []
    file = dummy()
    file.write = data.append
    MyElementTree(element).write(file, encoding)
    return string.join(data, "")

def MyghtyTag(name_, attrib_={}, **extra):
    """Can be used with ElementTree in places where Element is required"""
    element = et.Element(MyghtyTag, attrib_, **extra)
    element.name = name_
    return element

CODE_BLOCK = 'formatting.myt:code'
DOCTEST_DIRECTIVES = re.compile(r'#\s*doctest:\s*[+-]\w+(,[+-]\w+)*\s*$', re.M)
LINK = 'formatting.myt:link'
LINK_MARKER = 'rel:'
SECTION = 'doclib.myt:item'

def process_code_blocks(tree):
    """Replace <pre><code>...</code></pre> with Myghty tags, if it contains:
    * '{python}'
    or
    * '>>> '

    Note: '{python}' will be removed
    Note: also remove all doctest directives
    """
    parent = get_parent_map(tree)

    def replace_pre_with_myt(pre, text, type=None, title=None):
        text = re.sub(DOCTEST_DIRECTIVES, '', text)
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

        pre_parent = parent[pre]
        opts = {}
        if type == 'python':
            opts['syntaxtype'] = 'python'
        else:
            opts['syntaxtype'] = None

        if title is not None:
            opts['title'] = title
        
        if use_sliders:
            opts['use_sliders'] = True
            
        tag = MyghtyTag(CODE_BLOCK, opts)
        tag.text = text
        tag.tail = pre.tail
        pre_parent[index(pre_parent, pre)] = tag

    for precode in tree.findall('.//pre/code'):
        m = re.match(r'\{(python|code)(?: title="(.*?)"){0,1}\}', precode.text.lstrip())
        if m:
            code = m.group(1)
            title = m.group(2)
            text = precode.text.lstrip()
            text = re.sub(r'{(python|code).*?}(\n\s*)?', '', text)
            replace_pre_with_myt(parent[precode], text, type=code, title=title)
        elif precode.text.lstrip().startswith('>>> '):
            replace_pre_with_myt(parent[precode], precode.text)

def process_rel_href(tree):
    """Replace all <a href="rel:XXX">YYY</a> with Myghty tags

    If XXX is the same as YYY, text attribute for Myghty tag will not be
    provided.
    """
    parent = get_parent_map(tree)
    for a in tree.findall('.//a'):
        if a.get('href').startswith(LINK_MARKER):
            text = a.text
            path = a.get('href')[len(LINK_MARKER):]
            if text == path:
                tag = MyghtyTag(LINK, path=path)
            else:
                tag = MyghtyTag(LINK, path=path, text=text)
            tag.tail = a.tail
            a_parent = parent[a]
            a_parent[index(a_parent, a)] = tag

def process_headers(tree):
    """Replace all <h1>, <h2>... with Mighty tags
    """
    title = [None]
    def process(tree):
        while True:
            i = find_header_index(tree)
            if i is None:
                return
            node = tree[i]
            level = int(node.tag[1])
            start, end = i, end_of_header(tree, level, i+1)
            content = tree[start+1:end]
            description = node.text.strip()
            if title[0] is None:
                title[0] = description
            name = node.get('name')
            if name is None:
                name = description.split()[0].lower()

            tag = MyghtyTag(SECTION, name=name, description=description)
            tag.text = (node.tail or "") + '\n'
            tag.tail = '\n'
            tag[:] = content
            tree[start:end] = [tag]

            process(tag)

    process(tree)
    return title[0]

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

def get_parent_map(tree):
    return dict((c, p) for p in tree.getiterator() for c in p)
 
def html2myghtydoc(html):
    """Convert HTML to Myghty template (for SQLAlchemy's doc framework)

    html -- string containing HTML document, which:
    * should be without surrounding <html> or <body> tags
    * should have only one top-level header (usually <h1>). The text of the
      header will be used as a title of Myghty template.

    Things converter treats specially:
    * <a href="rel:XXX">YYY</a> will result in:
      <&formatting.myt:link, path="XXX", text="YYY"&>

      If XXX is the same as YYY, text attribute will be absent. E.g.
      <a href="rel:XXX">XXX</a> will result in:
      <&formatting.myt:link, path="XXX"&>

    * All header tags (<h1>, <h2>...) will be replaced. E.g. 

      <h2>Great title</h2><p>Section content goes here</p>

      will turn into:

      <&|doclib.myt:item, name="great", description="Great title"&>
      <p>Section content goes here</p>
      </&>

      Note that by default the value of `name` parameter is a lower-case version
      of the first word of the title. If you want to override it, supply
      `name` attribute to header tag, e.g.:
      <h2 name="title">Great title</h2>...
      will turn into
      <&|doclib.myt:item, name="title", description="Great title"&>...</&>

      (Note that to give this attribute in Markdown, you can
      write: {@name=title}.)

    * If you have {python} marker inside <code>, which is inside <pre>, it will
      replaced with Myghty tag. E.g.:

      <pre><code>{python} print 'hello, world!'</code></pre>

      will turn into

      <&|formatting.myt:code&>print 'hello, world!'</&>

      You don't need to write {python} marker if you use examples starting with
      Python prompt: >>>

      If you use doctest directives, they will be removed from output.

      (Note that <pre> and <code> is what Markdown outputs for pre-formatted
      code blocks.)
    """

    tree = et.fromstring("<html>" + html + "</html>")

    process_code_blocks(tree)
    process_rel_href(tree)
    title = process_headers(tree)

    header = "<%flags>inherit='document_base.myt'</%flags>\n"
    header += "<%attr>title='" + title + "'</%attr>\n"
    header += "<!-- WARNING! This file was automatically generated.\n" \
              "     Modify .txt file if need you to change the content.-->\n"

    # discard surrounding <html> tag
    body = ''.join(tostring(e) for e in tree[:])

    return header + body

if __name__ == '__main__':
    import glob
    filenames = sys.argv[1:]
    if len(filenames) == 0:
        filenames = glob.glob('content/*.txt')
    for inname in filenames:
        outname = inname[:-3] + 'myt'
        print inname, '->', outname
        input = file(inname).read()
        html = markdown.markdown(input)
        #file(inname[:-3] + "html", 'w').write(html)
        myt = html2myghtydoc(html)
        file(outname, 'w').write(myt)
