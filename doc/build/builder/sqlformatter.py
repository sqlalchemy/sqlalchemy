from pygments.lexer import RegexLexer, bygroups, using
from pygments.token import Token
from pygments.filter import Filter
from pygments.filter import apply_filters
from pygments.lexers import PythonLexer, PythonConsoleLexer
from sphinx.highlighting import PygmentsBridge
from pygments.formatters import HtmlFormatter, LatexFormatter

import re


def _strip_trailing_whitespace(iter_):
    buf = list(iter_)
    if buf:
        buf[-1] = (buf[-1][0], buf[-1][1].rstrip())
    for t, v in buf:
        yield t, v


class StripDocTestFilter(Filter):
    def filter(self, lexer, stream):
        for ttype, value in stream:
            if ttype is Token.Comment and re.match(r'#\s*doctest:', value):
                continue
            yield ttype, value

class PyConWithSQLLexer(RegexLexer):
    name = 'PyCon+SQL'
    aliases = ['pycon+sql']

    flags = re.IGNORECASE | re.DOTALL

    tokens = {
            'root': [
                (r'{sql}', Token.Sql.Link, 'sqlpopup'),
                (r'{opensql}', Token.Sql.Open, 'opensqlpopup'),
                (r'.*?\n', using(PythonConsoleLexer))
            ],
            'sqlpopup': [
                (
                    r'(.*?\n)((?:PRAGMA|BEGIN|SELECT|INSERT|DELETE|ROLLBACK|'
                        'COMMIT|ALTER|UPDATE|CREATE|DROP|PRAGMA'
                        '|DESCRIBE).*?(?:{stop}\n?|$))',
                    bygroups(using(PythonConsoleLexer), Token.Sql.Popup),
                    "#pop"
                )
            ],
            'opensqlpopup': [
                (
                    r'.*?(?:{stop}\n*|$)',
                    Token.Sql,
                    "#pop"
                )
            ]
        }


class PythonWithSQLLexer(RegexLexer):
    name = 'Python+SQL'
    aliases = ['pycon+sql']

    flags = re.IGNORECASE | re.DOTALL

    tokens = {
            'root': [
                (r'{sql}', Token.Sql.Link, 'sqlpopup'),
                (r'{opensql}', Token.Sql.Open, 'opensqlpopup'),
                (r'.*?\n', using(PythonLexer))
            ],
            'sqlpopup': [
                (
                    r'(.*?\n)((?:PRAGMA|BEGIN|SELECT|INSERT|DELETE|ROLLBACK'
                        '|COMMIT|ALTER|UPDATE|CREATE|DROP'
                        '|PRAGMA|DESCRIBE).*?(?:{stop}\n?|$))',
                    bygroups(using(PythonLexer), Token.Sql.Popup),
                    "#pop"
                )
            ],
            'opensqlpopup': [
                (
                    r'.*?(?:{stop}\n*|$)',
                    Token.Sql,
                    "#pop"
                )
            ]
        }

class PopupSQLFormatter(HtmlFormatter):
    def _format_lines(self, tokensource):
        buf = []
        for ttype, value in apply_filters(tokensource, [StripDocTestFilter()]):
            if ttype in Token.Sql:
                for t, v in HtmlFormatter._format_lines(self, iter(buf)):
                    yield t, v
                buf = []

                if ttype is Token.Sql:
                    yield 1, "<div class='show_sql'>%s</div>" % \
                                        re.sub(r'(?:[{stop}|\n]*)$', '', value)
                elif ttype is Token.Sql.Link:
                    yield 1, "<a href='#' class='sql_link'>sql</a>"
                elif ttype is Token.Sql.Popup:
                    yield 1, "<div class='popup_sql'>%s</div>" % \
                                        re.sub(r'(?:[{stop}|\n]*)$', '', value)
            else:
                buf.append((ttype, value))

        for t, v in _strip_trailing_whitespace(
                        HtmlFormatter._format_lines(self, iter(buf))):
            yield t, v

class PopupLatexFormatter(LatexFormatter):
    def _filter_tokens(self, tokensource):
        for ttype, value in apply_filters(tokensource, [StripDocTestFilter()]):
            if ttype in Token.Sql:
                if ttype is not Token.Sql.Link and ttype is not Token.Sql.Open:
                    yield Token.Literal, re.sub(r'{stop}', '', value)
                else:
                    continue
            else:
                yield ttype, value

    def format(self, tokensource, outfile):
        LatexFormatter.format(self, self._filter_tokens(tokensource), outfile)

def setup(app):
    app.add_lexer('pycon+sql', PyConWithSQLLexer())
    app.add_lexer('python+sql', PythonWithSQLLexer())

    PygmentsBridge.html_formatter = PopupSQLFormatter
    PygmentsBridge.latex_formatter = PopupLatexFormatter

