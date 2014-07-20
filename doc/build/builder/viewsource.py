from docutils import nodes
from sphinx.ext.viewcode import collect_pages
from sphinx.pycode import ModuleAnalyzer
import imp
from sphinx import addnodes
import re
from sphinx.util.compat import Directive
import os
from docutils.statemachine import StringList
from sphinx.environment import NoUri

import sys

py2k = sys.version_info < (3, 0)
if py2k:
    text_type = unicode
else:
    text_type = str

def view_source(name, rawtext, text, lineno, inliner,
                      options={}, content=[]):

    env = inliner.document.settings.env

    node = _view_source_node(env, text, None)
    return [node], []

def _view_source_node(env, text, state):
    # pretend we're using viewcode fully,
    # install the context it looks for
    if not hasattr(env, '_viewcode_modules'):
        env._viewcode_modules = {}

    modname = text
    text = modname.split(".")[-1] + ".py"

    # imitate sphinx .<modname> syntax
    if modname.startswith("."):
        # see if the modname needs to be corrected in terms
        # of current module context
        base_module = env.temp_data.get('autodoc:module')
        if base_module is None:
            base_module = env.temp_data.get('py:module')

        if base_module:
            modname = base_module + modname

    urito = env.app.builder.get_relative_uri

    # we're showing code examples which may have dependencies
    # which we really don't want to have required so load the
    # module by file, not import (though we are importing)
    # the top level module here...
    pathname = None
    for token in modname.split("."):
        file_, pathname, desc = imp.find_module(token, [pathname] if pathname else None)
        if file_:
            file_.close()

    # unlike viewcode which silently traps exceptions,
    # I want this to totally barf if the file can't be loaded.
    # a failed build better than a complete build missing
    # key content
    analyzer = ModuleAnalyzer.for_file(pathname, modname)
    # copied from viewcode
    analyzer.find_tags()
    if not isinstance(analyzer.code, text_type):
        code = analyzer.code.decode(analyzer.encoding)
    else:
        code = analyzer.code

    if state is not None:
        docstring = _find_mod_docstring(analyzer)
        if docstring:
            # get rid of "foo.py" at the top
            docstring = re.sub(r"^[a-zA-Z_0-9]+\.py", "", docstring)

            # strip
            docstring = docstring.strip()

            # yank only first paragraph
            docstring = docstring.split("\n\n")[0].strip()
    else:
        docstring = None

    entry = code, analyzer.tags, {}
    env._viewcode_modules[modname] = entry
    pagename = '_modules/' + modname.replace('.', '/')

    try:
        refuri = urito(env.docname, pagename)
    except NoUri:
        # if we're in the latex builder etc., this seems
        # to be what we get
        refuri = None


    if docstring:
        # embed the ref with the doc text so that it isn't
        # a separate paragraph
        if refuri:
            docstring = "`%s <%s>`_ - %s" % (text, refuri, docstring)
        else:
            docstring = "``%s`` - %s" % (text, docstring)
        para = nodes.paragraph('', '')
        state.nested_parse(StringList([docstring]), 0, para)
        return_node = para
    else:
        if refuri:
            refnode = nodes.reference('', '',
                    nodes.Text(text, text),
                    refuri=urito(env.docname, pagename)
                )
        else:
            refnode = nodes.Text(text, text)

        if state:
            return_node = nodes.paragraph('', '', refnode)
        else:
            return_node = refnode

    return return_node

from sphinx.pycode.pgen2 import token

def _find_mod_docstring(analyzer):
    """attempt to locate the module-level docstring.

    Note that sphinx autodoc just uses ``__doc__``.  But we don't want
    to import the module, so we need to parse for it.

    """
    analyzer.tokenize()
    for type_, parsed_line, start_pos, end_pos, raw_line in analyzer.tokens:
        if type_ == token.COMMENT:
            continue
        elif type_ == token.STRING:
            return eval(parsed_line)
        else:
            return None

def _parse_content(content):
    d = {}
    d['text'] = []
    idx = 0
    for line in content:
        idx += 1
        m = re.match(r' *\:(.+?)\:(?: +(.+))?', line)
        if m:
            attrname, value = m.group(1, 2)
            d[attrname] = value or ''
        else:
            break
    d["text"] = content[idx:]
    return d

def _comma_list(text):
    return re.split(r"\s*,\s*", text.strip())

class AutoSourceDirective(Directive):
    has_content = True

    def run(self):
        content = _parse_content(self.content)


        env = self.state.document.settings.env
        self.docname = env.docname

        sourcefile = self.state.document.current_source.split(os.pathsep)[0]
        dir_ = os.path.dirname(sourcefile)
        files = [
            f for f in os.listdir(dir_) if f.endswith(".py")
            and f != "__init__.py"
        ]

        if "files" in content:
            # ordered listing of files to include
            files = [fname for fname in _comma_list(content["files"])
                        if fname in set(files)]

        node = nodes.paragraph('', '',
                        nodes.Text("Listing of files:", "Listing of files:")
                )

        bullets = nodes.bullet_list()
        for fname in files:
            modname, ext = os.path.splitext(fname)
            # relative lookup
            modname = "." + modname

            link = _view_source_node(env, modname, self.state)

            list_node = nodes.list_item('',
                link
            )
            bullets += list_node

        node += bullets

        return [node]

def setup(app):
    app.add_role('viewsource', view_source)

    app.add_directive('autosource', AutoSourceDirective)

    # from sphinx.ext.viewcode
    app.connect('html-collect-pages', collect_pages)
