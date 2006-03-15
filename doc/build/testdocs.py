import os
import re
import doctest

def teststring(s, name, globs=None, verbose=None, report=True, 
               optionflags=0, extraglobs=None, raise_on_error=False, 
               parser=doctest.DocTestParser()):

    from doctest import DebugRunner, DocTestRunner, master

    # Assemble the globals.
    if globs is None:
        globs = {}
    else:
        globs = globs.copy()
    if extraglobs is not None:
        globs.update(extraglobs)

    if raise_on_error:
        runner = DebugRunner(verbose=verbose, optionflags=optionflags)
    else:
        runner = DocTestRunner(verbose=verbose, optionflags=optionflags)

    test = parser.get_doctest(s, globs, name, name, 0)
    runner.run(test)

    if report:
        runner.summarize()

    if master is None:
        master = runner
    else:
        master.merge(runner)

    return runner.failures, runner.tries

def replace_file(s, oldfile, newfile):
    engine = r"(^\s*>>>\s*[a-zA-Z_]\w*\s*=\s*create_engine\('sqlite',\s*\{'filename':\s*')" + oldfile+ "('\}\)$)"
    engine = re.compile(engine, re.MULTILINE)
    s, n = re.subn(engine, r'\1' + newfile + r'\2', s, 1)
    if not n:
        raise ValueError("Couldn't find suitable create_engine call to replace '%s' in it" % oldfile)
    return s

filename = 'content/tutorial.txt'
s = open(filename).read()
s = replace_file(s, 'tutorial.db', ':memory:')
teststring(s, filename)

