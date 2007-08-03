# $Id$
# highlight.py - syntax highlighting functions for Myghty 
# Copyright (C) 2004 Michael Bayer mike_mp@zzzcomputing.com
#
# This module is part of SQLAlchemy and is released under
# the MIT License: http://www.opensource.org/licenses/mit-license.php



import re, StringIO, sys, string, os
import token, tokenize, keyword

# Highlighter - highlights Myghty and Python source code

__all__ = ['highlight', 'PythonHighlighter', 'MyghtyHighlighter']

pystyles = {
    token.ENDMARKER : 'python_operator' ,
    token.NAME : 'python_name' ,
    token.NUMBER : 'python_number' ,
    token.STRING : 'python_literal' ,
    token.NEWLINE : 'python_operator' ,
    token.INDENT : 'python_operator' ,
    token.DEDENT : 'python_operator' ,
    token.LPAR : 'python_enclosure' ,
    token.RPAR : 'python_enclosure' ,
    token.LSQB : 'python_enclosure' ,
    token.RSQB : 'python_enclosure' ,
    token.COLON : 'python_operator' ,
    token.COMMA : 'python_operator' ,
    token.SEMI : 'python_operator' ,
    token.PLUS : 'python_operator' ,
    token.MINUS : 'python_operator' ,
    token.STAR : 'python_operator' ,
    token.SLASH : 'python_operator' ,
    token.VBAR : 'python_operator' ,
    token.AMPER : 'python_operator' ,
    token.LESS : 'python_operator' ,
    token.GREATER : 'python_operator' ,
    token.EQUAL : 'python_operator' ,
    token.DOT : 'python_operator' ,
    token.PERCENT : 'python_operator' ,
    token.BACKQUOTE : 'python_operator' ,
    token.LBRACE : 'python_enclosure',
    token.RBRACE : 'python_enclosure' ,
    token.EQEQUAL : 'python_operator' ,
    token.NOTEQUAL : 'python_operator' ,
    token.LESSEQUAL : 'python_operator' ,
    token.GREATEREQUAL : 'python_operator' ,
    token.TILDE : 'python_operator' ,
    token.CIRCUMFLEX : 'python_operator' ,
    token.LEFTSHIFT : 'python_operator' ,
    token.RIGHTSHIFT : 'python_operator' ,
    token.DOUBLESTAR : 'python_operator' ,
    token.PLUSEQUAL : 'python_operator' ,
    token.MINEQUAL : 'python_operator' ,
    token.STAREQUAL : 'python_operator' ,
    token.SLASHEQUAL : 'python_operator' ,
    token.PERCENTEQUAL : 'python_operator' ,
    token.AMPEREQUAL : 'python_operator' ,
    token.VBAREQUAL : 'python_operator' ,
    token.CIRCUMFLEXEQUAL : 'python_operator' ,
    token.LEFTSHIFTEQUAL : 'python_operator' ,
    token.RIGHTSHIFTEQUAL : 'python_operator' ,
    token.DOUBLESTAREQUAL : 'python_operator' ,
    token.DOUBLESLASH : 'python_operator' ,
    token.DOUBLESLASHEQUAL : 'python_operator' ,
    token.OP : 'python_operator' ,
    token.ERRORTOKEN : 'python_operator' ,
    token.N_TOKENS : 'python_operator' ,
    token.NT_OFFSET : 'python_operator' ,
    tokenize.COMMENT: 'python_comment',
    }

html_escapes = {
    '&' : '&amp;',
    '>' : '&gt;', 
    '<' : '&lt;', 
    '"' : '&quot;'
}

def do_html_escape(string):
    #return "@" + re.sub(r"([&<>])", lambda m: html_escapes[m.group()], string) + "+"
    return re.sub(r"([&<>])", lambda m: html_escapes[m.group()], string) 

def highlight(source, filename = None, syntaxtype = None, html_escape = True):
    if syntaxtype is not None:
        highlighter = highlighters.get(syntaxtype, None)
    elif filename is not None:
        (root, filename) = os.path.split(filename)
        highlighter = highlighters.get(filename, None)
        if highlighter is None:
            (root, ext) = os.path.splitext(filename)
            highlighter = highlighters.get(ext, None)
    else:
        highlighter = None    
        
    if highlighter is None:
        if html_escape:
            return do_html_escape(source)
        else:
            return source
    else:
        return highlighter(source, html_escape = html_escape).highlight()
        
class Highlighter:
    def __init__(self, source, output = None, html_escape = True):
        self.source = source
        self.pos = 0
        self.html_escape = html_escape
        if output is None:
            self.output = StringIO.StringIO()
        else:
            self.output = output
    
    def content(self):
        return self.output.getvalue()

    def highlight(self):raise NotImplementedError()

        
    def colorize(self, tokens):
        for pair in tokens:
            if pair[1] is None:
                if self.html_escape:
                    self.output.write(do_html_escape(pair[0]))
                else:
                    self.output.write(pair[0])
            else:
                if self.html_escape:
                    self.output.write('<span class="%s">%s</span>' % (pair[1], do_html_escape(pair[0])))
                else:
                    self.output.write('<span class="%s">%s</span>' % (pair[1], pair[0]))


class PythonHighlighter(Highlighter):

    def _line_grid(self, str, start, end):
        lines = re.findall(re.compile(r'[^\n]*\n?', re.S), str)
        r = 0
        for l in lines[0 : end[0] - start[0]]:
            r += len(l)
        r += end[1]
        return (start, (start[0], r))
        
    def highlight(self):
        buf = StringIO.StringIO(self.source)

        # tokenize module not too good at getting the 
        # whitespace at the end of a python block
        trailingspace = re.search(r"\n([ \t]+$)", self.source, re.S)
        if trailingspace:
            trailingspace = trailingspace.group(1) 

        curl = -1
        tokens =  []
        curstyle = None
        line = None

        for t in tokenize.generate_tokens(lambda: buf.readline()):
            if t[2][0] != curl:
                curl = t[2][0]
                curc = 0

            line = t[4]

            # pick up whitespace and output
            if t[2][1] > curc:
                tokens.append(line[curc : t[2][1]])
                curc = t[2][1]
    
            if self.get_style(t[0], t[1]) != curstyle:
                if tokens:
                    self.colorize([(string.join(tokens, ''), curstyle)])
                    tokens = []
                curstyle = self.get_style(t[0], t[1])

            (start, end) = self._line_grid(line, t[2], t[3])
            text = line[start[1]:end[1]]
            
            # special hardcoded rule to allow "interactive" demos without 
            # >>> getting sucked in as >> , > operators
            if text == '">>>"':
                text = '>>>'
            tokens.append(text) 
            curc = t[3][1]
            curl = t[3][0]

        # any remaining content to output, output it
        if tokens:
            self.colorize([(string.join(tokens, ''), curstyle)])

        if trailingspace:
            self.output.write(trailingspace)

        return self.content()

    def get_style(self, tokenid, str):
        if tokenid == token.NAME:
            if keyword.iskeyword(str):
                return "python_keyword"
            else:
                return "python_name"
        elif tokenid == token.OP:
            if "()[]{}".find(str) != -1:
                return "python_enclosure"
            else:
                return "python_operator"
        else:
            return pystyles.get(tokenid, None)

class MyghtyHighlighter(Highlighter):

    def _match(self, regexp):

        match = regexp.match(self.source, self.pos)
        if match:
            (start, end) = match.span()
            self.output.write(self.source[self.pos:start])
            
            if start == end:
                self.pos = end + 1
            else:
                self.pos = end
                
            return match
        else:
            return None

    
    def highlight(self):
        
        while (self.pos < len(self.source)):
            if self.match_named_block():
                continue
            
            if self.match_block():
                continue
                
            if self.match_comp_call():
                continue
            
            if self.match_comp_content_call():
                continue

            if self.match_substitution():
                continue
                
            if self.match_line():
                continue
                
            if self.match_text():
                continue;
                
            break

        return self.content()


    def pythonize(self, text):
        py = PythonHighlighter(text, output = self.output)
        py.highlight()

    def match_text(self):
        textmatch = re.compile(r"""
                (.*?)         # anything, followed by:
                (
                 (?<=\n)(?=[%#]) # an eval or comment line 
                 |
                 (?=</?[%&])  # a substitution or block or call start or end
                                              # - don't consume
                 |
                 (\\\n)         # an escaped newline  
                 |
                 \Z           # end of string
                )""", re.X | re.S)

        match = self._match(textmatch)
        if match:
            self.colorize([(match.group(1), 'text')])
            if match.group(3):
                self.colorize([(match.group(3), 'python_operator')])
            return True
        else:
            return False

    def match_named_block(self):
        namedmatch = re.compile(r"(<%(def|method))(.*?)(>)(.*?)(</%\2>)", re.M | re.S)
        
        match = self._match(namedmatch)
        if match:
            self.colorize([(match.group(1), 'deftag')])
            self.colorize([(match.group(3), 'compname')])
            self.colorize([(match.group(4), 'deftag')])
            MyghtyHighlighter(match.group(5), self.output).highlight()
            self.colorize([(match.group(6), 'deftag')])
            return True
        else:
            return False
    
    def match_block(self):
        blockmatch = re.compile(r"(<%(\w+).*?>)(.*?)(</%\2\s*>)", re.M | re.S)
        match = self._match(blockmatch)
    

        if match:
            style = {
                'doc': 'doctag',
                'args': 'argstag',        
            }.setdefault(match.group(2), "blocktag")

            self.colorize([(match.group(1), style)])
            if style == 'doctag':
                self.colorize([(match.group(3), 'doctag_text')])

            else:
                self.pythonize(match.group(3))
            self.colorize([(match.group(4), style)])

            return True
        else:
            return False

    def match_comp_call(self):
        compmatch = re.compile(r"(<&[^|])(.*?)(,.*?)?(&>)", re.M)
        match = self._match(compmatch)
        if match:
            self.colorize([(match.group(1), 'compcall')])
            self.colorize([(match.group(2), 'compname')])
            if match.group(3) is not None:
                self.pythonize(match.group(3))
            self.colorize([(match.group(4), 'compcall')])
            return True
        else:
            return False
            

    def match_substitution(self):
        submatch = re.compile(r"(<%)(.*?)(%>)", re.M)
        match = self._match(submatch)
        if match:
            self.colorize([(match.group(1), 'substitution')])
            self.pythonize(match.group(2))
            self.colorize([(match.group(3), 'substitution')])
            return True
        else:
            return False
        
    def match_comp_content_call(self):
        compcontmatch = re.compile(r"(<&\|)(.*?)(,.*?)?(&>)|(</&>)", re.M | re.S)
        match = self._match(compcontmatch)
        if match:
            if match.group(5) is not None:
                self.colorize([(match.group(5), 'compcall')])
            else:
                self.colorize([(match.group(1), 'compcall')])
                self.colorize([(match.group(2), 'compname')])
                if match.group(3) is not None:
                    self.pythonize(match.group(3))
                self.colorize([(match.group(4), 'compcall')]) 
            return True
        else:
            return False

    def match_line(self):
        linematch = re.compile(r"(?<=^)([%#])([^\n]*)(\n|\Z)", re.M)
        match = self._match(linematch)
        if match:
            if match.group(1) == '#':
                self.colorize([(match.group(0), 'doctag')])
            else:
                #self.colorize([(match.group(0), 'doctag')])
                self.colorize([(match.group(1), 'controlline')])
                self.pythonize(match.group(2))
                self.output.write(match.group(3))
            return True
        else:
            return False


highlighters = {
    '.myt': MyghtyHighlighter,
    '.myc': MyghtyHighlighter,
    'autohandler' : MyghtyHighlighter,
    'dhandler': MyghtyHighlighter,
    '.py': PythonHighlighter,
    'myghty': MyghtyHighlighter,
    'python' : PythonHighlighter
}
