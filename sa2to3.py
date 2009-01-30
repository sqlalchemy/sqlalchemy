"""SQLAlchemy 2to3 tool.

Relax !  This just calls the regular 2to3 tool with a preprocessor bolted onto it.


I originally wanted to write a custom fixer to accomplish this
but the Fixer classes seem like they can only understand 
the grammar file included with 2to3, and the grammar does not
seem to include Python comments (and of course, huge hacks needed
to get out-of-package fixers in there).   While that may be
an option later on this is a pretty simple approach for
what is a pretty simple problem.

"""

from lib2to3 import main, refactor

import re

py3k_pattern = re.compile(r'\s*# Py3K')
comment_pattern = re.compile(r'(\s*)#(?! ?Py2K)(.*)')
py2k_pattern = re.compile(r'\s*# Py2K')
end_py2k_pattern = re.compile(r'\s*# end Py2K')

def preprocess(data):
    lines = data.split('\n')
    def consume_normal():
        while lines:
            line = lines.pop(0)
            if py3k_pattern.match(line):
                for line in consume_py3k():
                    yield line
            elif py2k_pattern.match(line):
                for line in consume_py2k():
                    yield line
            else:
                yield line
    
    def consume_py3k():
        yield "# start Py3K"
        while lines:
            line = lines.pop(0)
            m = comment_pattern.match(line)
            if m:
                yield "%s%s" % m.group(1, 2)
            else:
                # pushback
                lines.insert(0, line)
                break
        yield "# end Py3K"
    
    def consume_py2k():
        yield "# start Py2K"
        while lines:
            line = lines.pop(0)
            if not end_py2k_pattern.match(line):
                yield "#%s" % line
            else:
                break
        yield "# end Py2K"

    return "\n".join(consume_normal())

old_refactor_string = main.StdoutRefactoringTool.refactor_string

def refactor_string(self, data, name):
    newdata = preprocess(data)
    tree = old_refactor_string(self, newdata, name)
    if tree:
        if newdata != data:
            tree.was_changed = True
    return tree
    
main.StdoutRefactoringTool.refactor_string = refactor_string

main.main("lib2to3.fixes")
