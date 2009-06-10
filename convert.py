import os
import subprocess
import re

def walk():
    for root, dirs, files in os.walk("./test/"):
        if root.endswith("/perf"):
            continue
        
        for fname in files:
            if not fname.endswith(".py"):
                continue
            if fname == "alltests.py":
                subprocess.call(["svn", "remove", os.path.join(root, fname)])
            elif fname.startswith("_") or fname == "__init__.py" or fname == "pickleable.py":
                convert(os.path.join(root, fname))
            elif not fname.startswith("test_"):
                if os.path.exists(os.path.join(root, "test_" + fname)):
                    os.unlink(os.path.join(root, "test_" + fname))
                subprocess.call(["svn", "rename", os.path.join(root, fname), os.path.join(root, "test_" + fname)])
                convert(os.path.join(root, "test_" + fname))


def convert(fname):
    lines = list(file(fname))
    replaced = []
    flags = {}
    
    while lines:
        for reg, handler in handlers:
            m = reg.match(lines[0])
            if m:
                handler(lines, replaced, flags)
                break
    
    post_handler(lines, replaced, flags)
    f = file(fname, 'w')
    f.write("".join(replaced))
    f.close()

handlers = []


def post_handler(lines, replaced, flags):
    imports = []
    if "needs_eq" in flags:
        imports.append("eq_")
    if "needs_assert_raises" in flags:
        imports += ["assert_raises", "assert_raises_message"]
    if imports:
        for i, line in enumerate(replaced):
            if "import" in line:
                replaced.insert(i, "from sqlalchemy.test.testing import %s\n" % ", ".join(imports))
                break
    
def remove_line(lines, replaced, flags):
    lines.pop(0)
    
handlers.append((re.compile(r"import testenv; testenv\.configure_for_tests"), remove_line))
handlers.append((re.compile(r"(.*\s)?import sa_unittest"), remove_line))


def import_testlib_sa(lines, replaced, flags):
    line = lines.pop(0)
    line = line.replace("import testlib.sa", "import sqlalchemy")
    replaced.append(line)
handlers.append((re.compile("import testlib\.sa"), import_testlib_sa))

def from_testlib_sa(lines, replaced, flags):
    line = lines.pop(0)
    while True:
        if line.endswith("\\\n"):
            line = line[0:-2] + lines.pop(0)
        else:
            break
    
    components = re.compile(r'from testlib\.sa import (.*)').match(line)
    if components:
        components = re.split(r"\s*,\s*", components.group(1))
        line = "from sqlalchemy import %s\n" % (", ".join(c for c in components if c not in ("Table", "Column")))
        replaced.append(line)
        if "Table" in components:
            replaced.append("from sqlalchemy.test.schema import Table\n")
        if "Column" in components:
            replaced.append("from sqlalchemy.test.schema import Column\n")
        return
        
    line = line.replace("testlib.sa", "sqlalchemy")
    replaced.append(line)
handlers.append((re.compile("from testlib\.sa.*import"), from_testlib_sa))

def from_testlib(lines, replaced, flags):
    line = lines.pop(0)
    
    components = re.compile(r'from testlib import (.*)').match(line)
    if components:
        components = re.split(r"\s*,\s*", components.group(1))
        if "sa" in components:
            replaced.append("import sqlalchemy as sa\n")
            replaced.append("from sqlalchemy.test import %s\n" % (", ".join(c for c in components if c != "sa" and c != "sa as tsa")))
            return
        elif "sa as tsa" in components:
            replaced.append("import sqlalchemy as tsa\n")
            replaced.append("from sqlalchemy.test import %s\n" % (", ".join(c for c in components if c != "sa" and c != "sa as tsa")))
            return
    
    line = line.replace("testlib", "sqlalchemy.test")
    replaced.append(line)
handlers.append((re.compile(r"from testlib"), from_testlib))

def from_orm(lines, replaced, flags):
    line = lines.pop(0)
    line = line.replace("from orm import", "from test.orm import")
    line = line.replace("from orm.", "from test.orm.")
    replaced.append(line)
handlers.append((re.compile(r'from orm( import|\.)'), from_orm))
    
def assert_equals(lines, replaced, flags):
    line = lines.pop(0)
    line = line.replace("self.assertEquals", "eq_")
    line = line.replace("self.assertEqual", "eq_")
    replaced.append(line)
    flags["needs_eq"] = True
handlers.append((re.compile(r"\s*self\.assertEqual(s)?"), assert_equals))

def assert_raises(lines, replaced, flags):
    line = lines.pop(0)
    line = line.replace("self.assertRaisesMessage", "assert_raises_message")
    line = line.replace("self.assertRaises", "assert_raises")
    replaced.append(line)
    flags["needs_assert_raises"] = True
handlers.append((re.compile(r"\s*self\.assertRaises(Message)?"), assert_raises))

def setup_all(lines, replaced, flags):
    line = lines.pop(0)
    whitespace = re.compile(r"(\s*)def setUpAll\(self\)\:").match(line).group(1)
    replaced.append("%s@classmethod\n" % whitespace)
    replaced.append("%sdef setup_class(cls):\n" % whitespace)
handlers.append((re.compile(r"\s*def setUpAll\(self\)"), setup_all))

def teardown_all(lines, replaced, flags):
    line = lines.pop(0)
    whitespace = re.compile(r"(\s*)def tearDownAll\(self\)\:").match(line).group(1)
    replaced.append("%s@classmethod\n" % whitespace)
    replaced.append("%sdef teardown_class(cls):\n" % whitespace)
handlers.append((re.compile(r"\s*def tearDownAll\(self\)"), teardown_all))

def setup(lines, replaced, flags):
    line = lines.pop(0)
    whitespace = re.compile(r"(\s*)def setUp\(self\)\:").match(line).group(1)
    replaced.append("%sdef setup(self):\n" % whitespace)
handlers.append((re.compile(r"\s*def setUp\(self\)"), setup))

def teardown(lines, replaced, flags):
    line = lines.pop(0)
    whitespace = re.compile(r"(\s*)def tearDown\(self\)\:").match(line).group(1)
    replaced.append("%sdef teardown(self):\n" % whitespace)
handlers.append((re.compile(r"\s*def tearDown\(self\)"), teardown))
    
def define_tables(lines, replaced, flags):
    line = lines.pop(0)
    whitespace = re.compile(r"(\s*)def define_tables").match(line).group(1)
    replaced.append("%s@classmethod\n" % whitespace)
    replaced.append("%sdef define_tables(cls, metadata):\n" % whitespace)
handlers.append((re.compile(r"\s*def define_tables\(self, metadata\)"), define_tables))

def setup_mappers(lines, replaced, flags):
    line = lines.pop(0)
    whitespace = re.compile(r"(\s*)def setup_mappers").match(line).group(1)
    
    i = -1
    while re.match("\s*@testing", replaced[i]):
        i -= 1
        
    replaced.insert(len(replaced) + i + 1, "%s@classmethod\n" % whitespace)
    replaced.append("%sdef setup_mappers(cls):\n" % whitespace)
handlers.append((re.compile(r"\s*def setup_mappers\(self\)"), setup_mappers))

def setup_classes(lines, replaced, flags):
    line = lines.pop(0)
    whitespace = re.compile(r"(\s*)def setup_classes").match(line).group(1)
    
    i = -1
    while re.match("\s*@testing", replaced[i]):
        i -= 1
        
    replaced.insert(len(replaced) + i + 1, "%s@classmethod\n" % whitespace)
    replaced.append("%sdef setup_classes(cls):\n" % whitespace)
handlers.append((re.compile(r"\s*def setup_classes\(self\)"), setup_classes))

def insert_data(lines, replaced, flags):
    line = lines.pop(0)
    whitespace = re.compile(r"(\s*)def insert_data").match(line).group(1)
    
    i = -1
    while re.match("\s*@testing", replaced[i]):
        i -= 1
        
    replaced.insert(len(replaced) + i + 1, "%s@classmethod\n" % whitespace)
    replaced.append("%sdef insert_data(cls):\n" % whitespace)
handlers.append((re.compile(r"\s*def insert_data\(self\)"), insert_data))

def fixtures(lines, replaced, flags):
    line = lines.pop(0)
    whitespace = re.compile(r"(\s*)def fixtures").match(line).group(1)
    
    i = -1
    while re.match("\s*@testing", replaced[i]):
        i -= 1
        
    replaced.insert(len(replaced) + i + 1, "%s@classmethod\n" % whitespace)
    replaced.append("%sdef fixtures(cls):\n" % whitespace)
handlers.append((re.compile(r"\s*def fixtures\(self\)"), fixtures))
    
    
def call_main(lines, replaced, flags):
    replaced.pop(-1)
    lines.pop(0)
handlers.append((re.compile(r"\s+testenv\.main\(\)"), call_main))

def default(lines, replaced, flags):
    replaced.append(lines.pop(0))
handlers.append((re.compile(r".*"), default))


if __name__ == '__main__':
    convert("test/orm/inheritance/abc_inheritance.py")
#    walk()
