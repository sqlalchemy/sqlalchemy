import inspect, re
import config, testing
from sqlalchemy import orm

__all__ = 'mapper',


_whitespace = re.compile(r'^(\s+)')

def _find_pragma(lines, current):
    m = _whitespace.match(lines[current])
    basis = m and m.group() or ''

    for line in reversed(lines[0:current]):
        if 'testlib.pragma' in line:
            return line
        m = _whitespace.match(line)
        indent = m and m.group() or ''

        # simplistic detection:

        # >> # testlib.pragma foo
        # >> center_line()
        if indent == basis:
            break
        # >> # testlib.pragma foo
        # >> if fleem:
        # >>     center_line()
        if line.endswith(':'):
            break
    return None

def _make_blocker(method_name, fallback):
    """Creates tripwired variant of a method, raising when called.

    To excempt an invocation from blockage, there are two options.

    1) add a pragma in a comment::

        # testlib.pragma exempt:methodname
        offending_line()

    2) add a magic cookie to the function's namespace::
        __sa_baremethodname_exempt__ = True
        ...
        offending_line()
        another_offending_lines()

    The second is useful for testing and development.
    """

    if method_name.startswith('__') and method_name.endswith('__'):
        frame_marker = '__sa_%s_exempt__' % method_name[2:-2]
    else:
        frame_marker = '__sa_%s_exempt__' % method_name
    pragma_marker = 'exempt:' + method_name

    def method(self, *args, **kw):
        frame_r = None
        try:
            frame = inspect.stack()[1][0]
            frame_r = inspect.getframeinfo(frame, 9)

            module = frame.f_globals.get('__name__', '')

            type_ = type(self)

            pragma = _find_pragma(*frame_r[3:5])

            exempt = (
                (not module.startswith('sqlalchemy')) or
                (pragma and pragma_marker in pragma) or
                (frame_marker in frame.f_locals) or
                ('self' in frame.f_locals and
                 getattr(frame.f_locals['self'], frame_marker, False)))

            if exempt:
                supermeth = getattr(super(type_, self), method_name, None)
                if (supermeth is None or
                    getattr(supermeth, 'im_func', None) is method):
                    return fallback(self, *args, **kw)
                else:
                    return supermeth(*args, **kw)
            else:
                raise AssertionError(
                    "%s.%s called in %s, line %s in %s" % (
                    type_.__name__, method_name, module, frame_r[1], frame_r[2]))
        finally:
            del frame
    method.__name__ = method_name
    return method

def mapper(type_, *args, **kw):
    forbidden = [
        ('__hash__', 'unhashable', lambda s: id(s)),
        ('__eq__', 'noncomparable', lambda s, o: s is o),
        ('__ne__', 'noncomparable', lambda s, o: s is not o),
        ('__cmp__', 'noncomparable', lambda s, o: object.__cmp__(s, o)),
        ('__le__', 'noncomparable', lambda s, o: object.__le__(s, o)),
        ('__lt__', 'noncomparable', lambda s, o: object.__lt__(s, o)),
        ('__ge__', 'noncomparable', lambda s, o: object.__ge__(s, o)),
        ('__gt__', 'noncomparable', lambda s, o: object.__gt__(s, o)),
        ('__nonzero__', 'truthless', lambda s: 1), ]

    if isinstance(type_, type) and type_.__bases__ == (object,):
        for method_name, option, fallback in forbidden:
            if (getattr(config.options, option, False) and
                method_name not in type_.__dict__):
                setattr(type_, method_name, _make_blocker(method_name, fallback))

    return orm.mapper(type_, *args, **kw)
