#!/usr/bin/env python

"""
====================================================================
IF YOU ARE LOOKING TO EXTEND MARKDOWN, SEE THE "FOOTNOTES" SECTION
====================================================================

Python-Markdown
===============

Converts Markdown to HTML.  Basic usage as a module:

    import markdown
    html = markdown.markdown(your_text_string)

Started by [Manfred Stienstra](http://www.dwerg.net/).  Continued and
maintained  by [Yuri Takhteyev](http://www.freewisdom.org).

Project website: http://www.freewisdom.org/projects/python-markdown
Contact: yuri [at] freewisdom.org

License: GPL 2 (http://www.gnu.org/copyleft/gpl.html) or BSD

Version: 1.3 (Feb. 28, 2006)

For changelog, see end of file
"""

import re, sys, os, random

# set debug level: 3 none, 2 critical, 1 informative, 0 all
(VERBOSE, INFO, CRITICAL, NONE) = range(4)

MESSAGE_THRESHOLD = CRITICAL

def message(level, text) :
    if level >= MESSAGE_THRESHOLD :
        print text


# --------------- CONSTANTS YOU MIGHT WANT TO MODIFY -----------------

# all tabs will be expanded to up to this many spaces
TAB_LENGTH = 4
ENABLE_ATTRIBUTES = 1


# --------------- CONSTANTS YOU _SHOULD NOT_ HAVE TO CHANGE ----------

FN_BACKLINK_TEXT = "zz1337820767766393qq"
# a template for html placeholders
HTML_PLACEHOLDER_PREFIX = "qaodmasdkwaspemas"
HTML_PLACEHOLDER = HTML_PLACEHOLDER_PREFIX + "%dajkqlsmdqpakldnzsdfls"

BLOCK_LEVEL_ELEMENTS = ['p', 'div', 'blockquote', 'pre', 'table',
                        'dl', 'ol', 'ul', 'script', 'noscript',
                        'form', 'fieldset', 'iframe', 'math', 'ins',
                        'del', 'hr', 'hr/']

def is_block_level (tag) :
    return ( (tag in BLOCK_LEVEL_ELEMENTS) or
             (tag[0] == 'h' and tag[1] in "0123456789") )

"""
======================================================================
========================== NANODOM ===================================
======================================================================

The three classes below implement some of the most basic DOM
methods.  I use this instead of minidom because I need a simpler
functionality and do not want to require additional libraries.

Importantly, NanoDom does not do normalization, which is what we
want. It also adds extra white space when converting DOM to string
"""


class Document :

    def appendChild(self, child) :
        self.documentElement = child
        child.parent = self
        self.entities = {}

    def createElement(self, tag) :
        el = Element(tag)
        el.doc = self
        return el

    def createTextNode(self, text) :
        node = TextNode(text)
        node.doc = self
        return node

    def createEntityReference(self, entity):
        if entity not in self.entities:
            self.entities[entity] = EntityReference(entity)
        return self.entities[entity]

    def toxml (self) :
        return self.documentElement.toxml()

    def normalizeEntities(self, text) :

        pairs = [ #("&", "&amp;"),
                  ("<", "&lt;"),
                  (">", "&gt;"),
                  ("\"", "&quot;")]

        for old, new in pairs :
            text = text.replace(old, new)
        return text

    def find(self, test) :
        return self.documentElement.find(test)

    def unlink(self) :
        self.documentElement.unlink()
        self.documentElement = None


class Element :

    def __init__ (self, tag) :
        self.type = "element"
        self.nodeName = tag
        self.attributes = []
        self.attribute_values = {}
        self.childNodes = []

    def unlink(self) :
        for child in self.childNodes :
            if child.type == "element" :
                child.unlink()
        self.childNodes = None

    def setAttribute(self, attr, value) :
        if not attr in self.attributes :
            self.attributes.append(attr)

        self.attribute_values[attr] = value

    def insertChild(self, position, child) :
        self.childNodes.insert(position, child)
        child.parent = self

    def removeChild(self, child) :
        self.childNodes.remove(child)

    def replaceChild(self, oldChild, newChild) :
        position = self.childNodes.index(oldChild)
        self.removeChild(oldChild)
        self.insertChild(position, newChild)

    def appendChild(self, child) :
        self.childNodes.append(child)
        child.parent = self

    def handleAttributes(self) :
        pass

    def find(self, test, depth=0) :
        """ Returns a list of descendants that pass the test function """
        matched_nodes = []
        for child in self.childNodes :
            if test(child) :
                matched_nodes.append(child)
            if child.type == "element" :
                matched_nodes += child.find(test, depth+1)
        return matched_nodes

    def toxml(self):
        if ENABLE_ATTRIBUTES :
            for child in self.childNodes:
                child.handleAttributes()
        buffer = ""
        if self.nodeName in ['h1', 'h2', 'h3', 'h4'] :
            buffer += "\n"
        elif self.nodeName in ['li'] :
            buffer += "\n "
        buffer += "<" + self.nodeName
        for attr in self.attributes :
            value = self.attribute_values[attr]
            value = self.doc.normalizeEntities(value)
            buffer += ' %s="%s"' % (attr, value)
        if self.childNodes :
            buffer += ">"
            for child in self.childNodes :
                buffer += child.toxml()
            if self.nodeName == 'p' :
                buffer += "\n"
            elif self.nodeName == 'li' :
                buffer += "\n "
            buffer += "</%s>" % self.nodeName
        else :
            buffer += "/>"
        if self.nodeName in ['p', 'li', 'ul', 'ol',
                             'h1', 'h2', 'h3', 'h4'] :
            buffer += "\n"
        return buffer


class TextNode :

    def __init__ (self, text) :
        self.type = "text"
        self.value = text
        self.attrRegExp = re.compile(r'\{@([^\}]*)=([^\}]*)}') # {@id=123}

    def attributeCallback(self, match) :
        self.parent.setAttribute(match.group(1), match.group(2))

    def handleAttributes(self) :
        self.value = self.attrRegExp.sub(self.attributeCallback, self.value)

    def toxml(self) :
        text = self.value
        if not text.startswith(HTML_PLACEHOLDER_PREFIX):
            if self.parent.nodeName == "p" :
                text = text.replace("\n", "\n   ")
            elif (self.parent.nodeName == "li"
                  and self.parent.childNodes[0]==self):
                text = "\n     " + text.replace("\n", "\n     ")
        text = self.doc.normalizeEntities(text)
        return text


class EntityReference:

    def __init__(self, entity):
        self.type = "entity_ref"
        self.entity = entity

    def handleAttributes(self):
        pass

    def toxml(self):
        return "&" + self.entity + ";"


"""
======================================================================
========================== PRE-PROCESSORS ============================
======================================================================

Preprocessors munge source text before we start doing anything too
complicated.

Each preprocessor implements a "run" method that takes a pointer to
a list of lines of the document, modifies it as necessary and
returns either the same pointer or a pointer to a new list.
"""

class HeaderPreprocessor :

    """
       Replaces underlined headers with hashed headers to avoid
       the nead for lookahead later.
    """

    def run (self, lines) :

        for i in range(len(lines)) :
            if not lines[i] :
                continue

            if (i+1 <= len(lines)
                  and lines[i+1]
                  and lines[i+1][0] in ['-', '=']) :

                underline = lines[i+1].strip()

                if underline == "="*len(underline) :
                    lines[i] = "# " + lines[i].strip()
                    lines[i+1] = ""
                elif underline == "-"*len(underline) :
                    lines[i] = "## " + lines[i].strip()
                    lines[i+1] = ""

        return lines

HEADER_PREPROCESSOR = HeaderPreprocessor()

class LinePreprocessor :
    """Deals with HR lines (needs to be done before processing lists)"""

    def run (self, lines) :
        for i in range(len(lines)) :
            if self._isLine(lines[i]) :
                lines[i] = "<hr />"
        return lines

    def _isLine(self, block) :
        """Determines if a block should be replaced with an <HR>"""
        if block.startswith("    ") : return 0  # a code block
        text = "".join([x for x in block if not x.isspace()])
        if len(text) <= 2 :
            return 0
        for pattern in ['isline1', 'isline2', 'isline3'] :
            m = RE.regExp[pattern].match(text)
            if (m and m.group(1)) :
                return 1
        else:
            return 0

LINE_PREPROCESSOR = LinePreprocessor()


class LineBreaksPreprocessor :
    """Replaces double spaces at the end of the lines with <br/ >."""

    def run (self, lines) :
        for i in range(len(lines)) :
            if (lines[i].endswith("  ")
                and not RE.regExp['tabbed'].match(lines[i]) ):
                lines[i] += "<br />"
        return lines

LINE_BREAKS_PREPROCESSOR = LineBreaksPreprocessor()


class HtmlBlockPreprocessor :
    """Removes html blocks from self.lines"""

    def run (self, lines) :
        new_blocks = []
        text = "\n".join(lines)
        for block in text.split("\n\n") :
            if block.startswith("\n") :
                block = block[1:]
            if ( (block.startswith("<") and block.rstrip().endswith(">"))
                 and (block[1] in ["!", "?", "@", "%"]
                      or is_block_level( block[1:].replace(">", " ")
                                         .split()[0].lower()))) :
                new_blocks.append(
                    self.stash.store(block.strip()))
            else :
                new_blocks.append(block)
        return "\n\n".join(new_blocks).split("\n")

HTML_BLOCK_PREPROCESSOR = HtmlBlockPreprocessor()


class ReferencePreprocessor :

    def run (self, lines) :
        new_text = [];
        for line in lines:
            m = RE.regExp['reference-def'].match(line)
            if m:
                id = m.group(2).strip().lower()
                title = dequote(m.group(4).strip()) #.replace('"', "&quot;")
                self.references[id] = (m.group(3), title)
            else:
                new_text.append(line)
        return new_text #+ "\n"

REFERENCE_PREPROCESSOR = ReferencePreprocessor()

"""
======================================================================
========================== INLINE PATTERNS ===========================
======================================================================

Inline patterns such as *emphasis* are handled by means of auxiliary
objects, one per pattern.  Each pattern object uses a single regular
expression and needs support the following methods:

  pattern.getCompiledRegExp() - returns a regular expression

  pattern.handleMatch(m, doc) - takes a match object and returns
                                a NanoDom node (as a part of the provided
                                doc) or None

All of python markdown's built-in patterns subclass from BasePatter,
but you can add additional patterns that don't.

Also note that all the regular expressions used by inline must
capture the whole block.  For this reason, they all start with
'^(.*)' and end with '(.*)!'.  In case with built-in expression
BasePattern takes care of adding the "^(.*)" and "(.*)!".

Finally, the order in which regular expressions are applied is very
important - e.g. if we first replace http://.../ links with <a> tags
and _then_ try to replace inline html, we would end up with a mess.
So, we apply the expressions in the following order:

       * escape and backticks have to go before everything else, so
         that we can preempt any markdown patterns by escaping them.

       * then we handle auto-links (must be done before inline html)

       * then we handle inline HTML.  At this point we will simply
         replace all inline HTML strings with a placeholder and add
         the actual HTML to a hash.

       * then inline images (must be done before links)

       * then bracketed links, first regular then reference-style

       * finally we apply strong and emphasis
"""

NOBRACKET = r'[^\]\[]*'
BRK = ( r'\[('
        + (NOBRACKET + r'(\['+NOBRACKET)*6
        + (NOBRACKET+ r'\])*'+NOBRACKET)*6
        + NOBRACKET + r')\]' )

BACKTICK_RE = r'\`([^\`]*)\`'                    # `e= m*c^2`
DOUBLE_BACKTICK_RE =  r'\`\`(.*)\`\`'            # ``e=f("`")``
ESCAPE_RE = r'\\(.)'                             # \<
EMPHASIS_RE = r'\*([^\*]*)\*'                    # *emphasis*
EMPHASIS_2_RE = r'_([^_]*)_'                     # _emphasis_
LINK_RE = BRK + r'\s*\(([^\)]*)\)'               # [text](url)
LINK_ANGLED_RE = BRK + r'\s*\(<([^\)]*)>\)'      # [text](<url>)
IMAGE_LINK_RE = r'\!' + BRK + r'\s*\(([^\)]*)\)' # ![alttxt](http://x.com/)
REFERENCE_RE = BRK+ r'\s*\[([^\]]*)\]'           # [Google][3]
IMAGE_REFERENCE_RE = r'\!' + BRK + '\s*\[([^\]]*)\]' # ![alt text][2]
NOT_STRONG_RE = r'( \* )'                        # stand-alone * or _
STRONG_RE = r'\*\*(.*)\*\*'                      # **strong**
STRONG_2_RE = r'__([^_]*)__'                     # __strong__
STRONG_EM_RE = r'\*\*\*([^_]*)\*\*\*'            # ***strong***
STRONG_EM_2_RE = r'___([^_]*)___'                # ___strong___
AUTOLINK_RE = r'<(http://[^>]*)>'                # <http://www.123.com>
AUTOMAIL_RE = r'<([^> ]*@[^> ]*)>'               # <me@example.com>
HTML_RE = r'(\<[^\>]*\>)'                        # <...>
ENTITY_RE = r'(&[\#a-zA-Z0-9]*;)'                # &amp;

class BasePattern:

    def __init__ (self, pattern) :
        self.pattern = pattern
        self.compiled_re = re.compile("^(.*)%s(.*)$" % pattern, re.DOTALL)

    def getCompiledRegExp (self) :
        return self.compiled_re

class SimpleTextPattern (BasePattern) :

    def handleMatch(self, m, doc) :
        return doc.createTextNode(m.group(2))

class SimpleTagPattern (BasePattern):

    def __init__ (self, pattern, tag) :
        BasePattern.__init__(self, pattern)
        self.tag = tag

    def handleMatch(self, m, doc) :
        el = doc.createElement(self.tag)
        el.appendChild(doc.createTextNode(m.group(2)))
        return el

class BacktickPattern (BasePattern):

    def __init__ (self, pattern):
        BasePattern.__init__(self, pattern)
        self.tag = "code"

    def handleMatch(self, m, doc) :
        el = doc.createElement(self.tag)
        text = m.group(2).strip()
        text = text.replace("&", "&amp;")
        el.appendChild(doc.createTextNode(text))
        return el


class DoubleTagPattern (SimpleTagPattern) :

    def handleMatch(self, m, doc) :
        tag1, tag2 = self.tag.split(",")
        el1 = doc.createElement(tag1)
        el2 = doc.createElement(tag2)
        el1.appendChild(el2)
        el2.appendChild(doc.createTextNode(m.group(2)))
        return el1


class HtmlPattern (BasePattern):

    def handleMatch (self, m, doc) :
        place_holder = self.stash.store(m.group(2))
        return doc.createTextNode(place_holder)


class LinkPattern (BasePattern):

    def handleMatch(self, m, doc) :
        el = doc.createElement('a')
        el.appendChild(doc.createTextNode(m.group(2)))
        parts = m.group(9).split()
        # We should now have [], [href], or [href, title]
        if parts :
            el.setAttribute('href', parts[0])
        else :
            el.setAttribute('href', "")
        if len(parts) > 1 :
            # we also got a title
            title = " ".join(parts[1:]).strip()
            title = dequote(title) #.replace('"', "&quot;")
            el.setAttribute('title', title)
        return el


class ImagePattern (BasePattern):

    def handleMatch(self, m, doc):
        el = doc.createElement('img')
        src_parts = m.group(9).split()
        el.setAttribute('src', src_parts[0])
        if len(src_parts) > 1 :
            el.setAttribute('title', dequote(" ".join(src_parts[1:])))
        if ENABLE_ATTRIBUTES :
            text = doc.createTextNode(m.group(2))
            el.appendChild(text)
            text.handleAttributes()
            truealt = text.value
            el.childNodes.remove(text)
        else:
            truealt = m.group(2)
        el.setAttribute('alt', truealt)
        return el

class ReferencePattern (BasePattern):

    def handleMatch(self, m, doc):
        if m.group(9) :
            id = m.group(9).lower()
        else :
            # if we got something like "[Google][]"
            # we'll use "google" as the id
            id = m.group(2).lower()
        if not self.references.has_key(id) : # ignore undefined refs
            return None
        href, title = self.references[id]
        text = m.group(2)
        return self.makeTag(href, title, text, doc)

    def makeTag(self, href, title, text, doc):
        el = doc.createElement('a')
        el.setAttribute('href', href)
        if title :
            el.setAttribute('title', title)
        el.appendChild(doc.createTextNode(text))
        return el


class ImageReferencePattern (ReferencePattern):

    def makeTag(self, href, title, text, doc):
        el = doc.createElement('img')
        el.setAttribute('src', href)
        if title :
            el.setAttribute('title', title)
        el.setAttribute('alt', text)
        return el


class AutolinkPattern (BasePattern):

    def handleMatch(self, m, doc):
        el = doc.createElement('a')
        el.setAttribute('href', m.group(2))
        el.appendChild(doc.createTextNode(m.group(2)))
        return el

class AutomailPattern (BasePattern):

    def handleMatch(self, m, doc) :
        el = doc.createElement('a')
        email = m.group(2)
        if email.startswith("mailto:"):
            email = email[len("mailto:"):]
        for letter in email:
            entity = doc.createEntityReference("#%d" % ord(letter))
            el.appendChild(entity)
        mailto = "mailto:" + email
        mailto = "".join(['&#%d;' % ord(letter) for letter in mailto])
        el.setAttribute('href', mailto)
        return el

ESCAPE_PATTERN          = SimpleTextPattern(ESCAPE_RE)
NOT_STRONG_PATTERN      = SimpleTextPattern(NOT_STRONG_RE)

BACKTICK_PATTERN        = BacktickPattern(BACKTICK_RE)
DOUBLE_BACKTICK_PATTERN = BacktickPattern(DOUBLE_BACKTICK_RE)
STRONG_PATTERN          = SimpleTagPattern(STRONG_RE, 'strong')
STRONG_PATTERN_2        = SimpleTagPattern(STRONG_2_RE, 'strong')
EMPHASIS_PATTERN        = SimpleTagPattern(EMPHASIS_RE, 'em')
EMPHASIS_PATTERN_2      = SimpleTagPattern(EMPHASIS_2_RE, 'em')

STRONG_EM_PATTERN       = DoubleTagPattern(STRONG_EM_RE, 'strong,em')
STRONG_EM_PATTERN_2     = DoubleTagPattern(STRONG_EM_2_RE, 'strong,em')

LINK_PATTERN            = LinkPattern(LINK_RE)
LINK_ANGLED_PATTERN     = LinkPattern(LINK_ANGLED_RE)
IMAGE_LINK_PATTERN      = ImagePattern(IMAGE_LINK_RE)
IMAGE_REFERENCE_PATTERN = ImageReferencePattern(IMAGE_REFERENCE_RE)
REFERENCE_PATTERN       = ReferencePattern(REFERENCE_RE)

HTML_PATTERN            = HtmlPattern(HTML_RE)
ENTITY_PATTERN          = HtmlPattern(ENTITY_RE)

AUTOLINK_PATTERN        = AutolinkPattern(AUTOLINK_RE)
AUTOMAIL_PATTERN        = AutomailPattern(AUTOMAIL_RE)


"""
======================================================================
========================== POST-PROCESSORS ===========================
======================================================================

Markdown also allows post-processors, which are similar to
preprocessors in that they need to implement a "run" method.  Unlike
pre-processors, they take a NanoDom document as a parameter and work
with that.
#
There are currently no standard post-processors, but the footnote
extension below uses one.
"""
"""
======================================================================
========================== MISC AUXILIARY CLASSES ====================
======================================================================
"""

class HtmlStash :
    """This class is used for stashing HTML objects that we extract
        in the beginning and replace with place-holders."""

    def __init__ (self) :
        self.html_counter = 0 # for counting inline html segments
        self.rawHtmlBlocks=[]

    def store(self, html) :
        """Saves an HTML segment for later reinsertion.  Returns a
           placeholder string that needs to be inserted into the
           document.

           @param html: an html segment
           @returns : a placeholder string """
        self.rawHtmlBlocks.append(html)
        placeholder = HTML_PLACEHOLDER % self.html_counter
        self.html_counter += 1
        return placeholder


class BlockGuru :

    def _findHead(self, lines, fn, allowBlank=0) :

        """Functional magic to help determine boundaries of indented
           blocks.

           @param lines: an array of strings
           @param fn: a function that returns a substring of a string
                      if the string matches the necessary criteria
           @param allowBlank: specifies whether it's ok to have blank
                      lines between matching functions
           @returns: a list of post processes items and the unused
                      remainder of the original list"""

        items = []
        item = -1

        i = 0 # to keep track of where we are

        for line in lines :

            if not line.strip() and not allowBlank:
                return items, lines[i:]

            if not line.strip() and allowBlank:
                # If we see a blank line, this _might_ be the end
                i += 1

                # Find the next non-blank line
                for j in range(i, len(lines)) :
                    if lines[j].strip() :
                        next = lines[j]
                        break
                else :
                    # There is no more text => this is the end
                    break

                # Check if the next non-blank line is still a part of the list

                part = fn(next)

                if part :
                    items.append("")
                    continue
                else :
                    break # found end of the list

            part = fn(line)

            if part :
                items.append(part)
                i += 1
                continue
            else :
                return items, lines[i:]
        else :
            i += 1

        return items, lines[i:]


    def detabbed_fn(self, line) :
        """ An auxiliary method to be passed to _findHead """
        m = RE.regExp['tabbed'].match(line)
        if m:
            return m.group(4)
        else :
            return None


    def detectTabbed(self, lines) :

        return self._findHead(lines, self.detabbed_fn,
                              allowBlank = 1)


def print_error(string):
    """Print an error string to stderr"""
    sys.stderr.write(string +'\n')


def dequote(string) :
    """ Removes quotes from around a string """
    if ( ( string.startswith('"') and string.endswith('"'))
         or (string.startswith("'") and string.endswith("'")) ) :
        return string[1:-1]
    else :
        return string

"""
======================================================================
========================== CORE MARKDOWN =============================
======================================================================

This stuff is ugly, so if you are thinking of extending the syntax,
see first if you can do it via pre-processors, post-processors,
inline patterns or a combination of the three.
"""

class CorePatterns :
    """This class is scheduled for removal as part of a refactoring
        effort."""

    patterns = {
        'header':          r'(#*)([^#]*)(#*)', # # A title
        'reference-def' :  r'(\ ?\ ?\ ?)\[([^\]]*)\]:\s*([^ ]*)(.*)',
                           # [Google]: http://www.google.com/
        'containsline':    r'([-]*)$|^([=]*)', # -----, =====, etc.
        'ol':              r'[ ]{0,3}[\d]*\.\s+(.*)', # 1. text
        'ul':              r'[ ]{0,3}[*+-]\s+(.*)', # "* text"
        'isline1':         r'(\**)', # ***
        'isline2':         r'(\-*)', # ---
        'isline3':         r'(\_*)', # ___
        'tabbed':          r'((\t)|(    ))(.*)', # an indented line
        'quoted' :         r'> ?(.*)', # a quoted block ("> ...")
    }

    def __init__ (self) :

        self.regExp = {}
        for key in self.patterns.keys() :
            self.regExp[key] = re.compile("^%s$" % self.patterns[key],
                                          re.DOTALL)

        self.regExp['containsline'] = re.compile(r'^([-]*)$|^([=]*)$', re.M)

RE = CorePatterns()


class Markdown:
    """ Markdown formatter class for creating an html document from
        Markdown text """


    def __init__(self, source=None):
        """Creates a new Markdown instance.

           @param source: The text in Markdown format. """

        self.source = source
        self.blockGuru = BlockGuru()
        self.registeredExtensions = []

        self.preprocessors = [ HEADER_PREPROCESSOR,
                               LINE_PREPROCESSOR,
                               HTML_BLOCK_PREPROCESSOR,
                               LINE_BREAKS_PREPROCESSOR,
                               # A footnote preprocessor will
                               # get inserted here
                               REFERENCE_PREPROCESSOR ]


        self.postprocessors = [] # a footnote postprocessor will get
                                 # inserted later

        self.inlinePatterns = [ DOUBLE_BACKTICK_PATTERN,
                                BACKTICK_PATTERN,
                                ESCAPE_PATTERN,
                                IMAGE_LINK_PATTERN,
                                IMAGE_REFERENCE_PATTERN,
                                REFERENCE_PATTERN,
                                LINK_ANGLED_PATTERN,
                                LINK_PATTERN,
                                AUTOLINK_PATTERN,
                                AUTOMAIL_PATTERN,
                                HTML_PATTERN,
                                ENTITY_PATTERN,
                                NOT_STRONG_PATTERN,
                                STRONG_EM_PATTERN,
                                STRONG_EM_PATTERN_2,
                                STRONG_PATTERN,
                                STRONG_PATTERN_2,
                                EMPHASIS_PATTERN,
                                EMPHASIS_PATTERN_2
                                # The order of the handlers matters!!!
                                ]

        self.reset()

    def registerExtension(self, extension) :
        self.registeredExtensions.append(extension)

    def reset(self) :
        """Resets all state variables so that we can start
            with a new text."""
        self.references={}
        self.htmlStash = HtmlStash()

        HTML_BLOCK_PREPROCESSOR.stash = self.htmlStash
        REFERENCE_PREPROCESSOR.references = self.references
        HTML_PATTERN.stash = self.htmlStash
        ENTITY_PATTERN.stash = self.htmlStash
        REFERENCE_PATTERN.references = self.references
        IMAGE_REFERENCE_PATTERN.references = self.references

        for extension in self.registeredExtensions :
            extension.reset()


    def _transform(self):
        """Transforms the Markdown text into a XHTML body document

           @returns: A NanoDom Document """

        # Setup the document

        self.doc = Document()
        self.top_element = self.doc.createElement("span")
        self.top_element.appendChild(self.doc.createTextNode('\n'))
        self.top_element.setAttribute('class', 'markdown')
        self.doc.appendChild(self.top_element)

        # Fixup the source text
        text = self.source.strip()
        text = text.replace("\r\n", "\n").replace("\r", "\n")
        text += "\n\n"
        text = text.expandtabs(TAB_LENGTH)

        # Split into lines and run the preprocessors that will work with
        # self.lines

        self.lines = text.split("\n")

        # Run the pre-processors on the lines
        for prep in self.preprocessors :
            self.lines = prep.run(self.lines)

        # Create a NanoDom tree from the lines and attach it to Document
        self._processSection(self.top_element, self.lines)

        # Not sure why I put this in but let's leave it for now.
        self.top_element.appendChild(self.doc.createTextNode('\n'))

        # Run the post-processors
        for postprocessor in self.postprocessors :
            postprocessor.run(self.doc)

        return self.doc


    def _processSection(self, parent_elem, lines,
                        inList = 0, looseList = 0) :

        """Process a section of a source document, looking for high
           level structural elements like lists, block quotes, code
           segments, html blocks, etc.  Some those then get stripped
           of their high level markup (e.g. get unindented) and the
           lower-level markup is processed recursively.

           @param parent_elem: A NanoDom element to which the content
                               will be added
           @param lines: a list of lines
           @param inList: a level
           @returns: None"""

        if not lines :
            return

        # Check if this section starts with a list, a blockquote or
        # a code block

        processFn = { 'ul' :     self._processUList,
                      'ol' :     self._processOList,
                      'quoted' : self._processQuote,
                      'tabbed' : self._processCodeBlock }

        for regexp in ['ul', 'ol', 'quoted', 'tabbed'] :
            m = RE.regExp[regexp].match(lines[0])
            if m :
                processFn[regexp](parent_elem, lines, inList)
                return

        # We are NOT looking at one of the high-level structures like
        # lists or blockquotes.  So, it's just a regular paragraph
        # (though perhaps nested inside a list or something else).  If
        # we are NOT inside a list, we just need to look for a blank
        # line to find the end of the block.  If we ARE inside a
        # list, however, we need to consider that a sublist does not
        # need to be separated by a blank line.  Rather, the following
        # markup is legal:
        #
        # * The top level list item
        #
        #     Another paragraph of the list.  This is where we are now.
        #     * Underneath we might have a sublist.
        #

        if inList :

            start, theRest = self._linesUntil(lines, (lambda line:
                             RE.regExp['ul'].match(line)
                             or RE.regExp['ol'].match(line)
                                              or not line.strip()))

            self._processSection(parent_elem, start,
                                 inList - 1, looseList = looseList)
            self._processSection(parent_elem, theRest,
                                 inList - 1, looseList = looseList)


        else : # Ok, so it's just a simple block

            paragraph, theRest = self._linesUntil(lines, lambda line:
                                                 not line.strip())

            if len(paragraph) and paragraph[0].startswith('#') :
                m = RE.regExp['header'].match(paragraph[0])
                if m :
                    level = len(m.group(1))
                    h = self.doc.createElement("h%d" % level)
                    parent_elem.appendChild(h)
                    for item in self._handleInline(m.group(2)) :
                        h.appendChild(item)
                else :
                    message(CRITICAL, "We've got a problem header!")

            elif paragraph :

                list = self._handleInline("\n".join(paragraph))

                if ( parent_elem.nodeName == 'li'
                     and not (looseList or parent_elem.childNodes)):

                    #and not parent_elem.childNodes) :
                    # If this is the first paragraph inside "li", don't
                    # put <p> around it - append the paragraph bits directly
                    # onto parent_elem
                    el = parent_elem
                else :
                    # Otherwise make a "p" element
                    el = self.doc.createElement("p")
                    parent_elem.appendChild(el)

                for item in list :
                    el.appendChild(item)

            if theRest :
                theRest = theRest[1:]  # skip the first (blank) line

            self._processSection(parent_elem, theRest, inList)



    def _processUList(self, parent_elem, lines, inList) :
        self._processList(parent_elem, lines, inList,
                         listexpr='ul', tag = 'ul')

    def _processOList(self, parent_elem, lines, inList) :
        self._processList(parent_elem, lines, inList,
                         listexpr='ol', tag = 'ol')


    def _processList(self, parent_elem, lines, inList, listexpr, tag) :
        """Given a list of document lines starting with a list item,
           finds the end of the list, breaks it up, and recursively
           processes each list item and the remainder of the text file.

           @param parent_elem: A dom element to which the content will be added
           @param lines: a list of lines
           @param inList: a level
           @returns: None"""

        ul = self.doc.createElement(tag)  # ul might actually be '<ol>'
        parent_elem.appendChild(ul)

        looseList = 0

        # Make a list of list items
        items = []
        item = -1

        i = 0  # a counter to keep track of where we are

        for line in lines :

            loose = 0
            if not line.strip() :
                # If we see a blank line, this _might_ be the end of the list
                i += 1
                loose = 1

                # Find the next non-blank line
                for j in range(i, len(lines)) :
                    if lines[j].strip() :
                        next = lines[j]
                        break
                else :
                    # There is no more text => end of the list
                    break

                # Check if the next non-blank line is still a part of the list
                if ( RE.regExp[listexpr].match(next) or
                     RE.regExp['tabbed'].match(next) ):
                    # get rid of any white space in the line
                    items[item].append(line.strip())
                    looseList = loose or looseList
                    continue
                else :
                    break # found end of the list

            # Now we need to detect list items (at the current level)
            # while also detabing child elements if necessary

            for expr in [listexpr, 'tabbed']:

                m = RE.regExp[expr].match(line)
                if m :
                    if expr == listexpr :  # We are looking at a new item
                        if m.group(1) :
                            items.append([m.group(1)])
                            item += 1
                    elif expr == 'tabbed' :  # This line needs to be detabbed
                        items[item].append(m.group(4)) #after the 'tab'

                    i += 1
                    break
            else :
                items[item].append(line)  # Just regular continuation
                i += 1 # added on 2006.02.25
        else :
            i += 1

        # Add the dom elements
        for item in items :
            li = self.doc.createElement("li")
            ul.appendChild(li)

            self._processSection(li, item, inList + 1, looseList = looseList)

        # Process the remaining part of the section

        self._processSection(parent_elem, lines[i:], inList)


    def _linesUntil(self, lines, condition) :
        """ A utility function to break a list of lines upon the
            first line that satisfied a condition.  The condition
            argument should be a predicate function.
            """

        i = -1
        for line in lines :
            i += 1
            if condition(line) : break
        else :
            i += 1
        return lines[:i], lines[i:]

    def _processQuote(self, parent_elem, lines, inList) :
        """Given a list of document lines starting with a quote finds
           the end of the quote, unindents it and recursively
           processes the body of the quote and the remainder of the
           text file.

           @param parent_elem: DOM element to which the content will be added
           @param lines: a list of lines
           @param inList: a level
           @returns: None """

        dequoted = []
        i = 0
        for line in lines :
            m = RE.regExp['quoted'].match(line)
            if m :
                dequoted.append(m.group(1))
                i += 1
            else :
                break
        else :
            i += 1

        blockquote = self.doc.createElement('blockquote')
        parent_elem.appendChild(blockquote)

        self._processSection(blockquote, dequoted, inList)
        self._processSection(parent_elem, lines[i:], inList)




    def _processCodeBlock(self, parent_elem, lines, inList) :
        """Given a list of document lines starting with a code block
           finds the end of the block, puts it into the dom verbatim
           wrapped in ("<pre><code>") and recursively processes the
           the remainder of the text file.

           @param parent_elem: DOM element to which the content will be added
           @param lines: a list of lines
           @param inList: a level
           @returns: None"""

        detabbed, theRest = self.blockGuru.detectTabbed(lines)

        pre = self.doc.createElement('pre')
        code = self.doc.createElement('code')
        parent_elem.appendChild(pre)
        pre.appendChild(code)
        text = "\n".join(detabbed).rstrip()+"\n"
        text = text.replace("&", "&amp;")
        code.appendChild(self.doc.createTextNode(text))
        self._processSection(parent_elem, theRest, inList)


    def _handleInline(self,  line):
        """Transform a Markdown line with inline elements to an XHTML fragment.

        Note that this function works recursively: we look for a
        pattern, which usually splits the paragraph in half, and then
        call this function on the two parts.

        This function uses auxiliary objects called inline patterns.
        See notes on inline patterns above.

        @param item: A block of Markdown text
        @return: A list of NanoDomnodes """
        if not(line):
            return [self.doc.createTextNode(' ')]
        # two spaces at the end of the line denote a <br/>
        #if line.endswith('  '):
        #    list = self._handleInline( line.rstrip())
        #    list.append(self.doc.createElement('br'))
        #    return list
        #
        # ::TODO:: Replace with a preprocessor

        for pattern in self.inlinePatterns :
            list = self._applyPattern( line, pattern)
            if list: return list

        return [self.doc.createTextNode(line)]

    def _applyPattern(self,  line, pattern) :
        """ Given a pattern name, this function checks if the line
            fits the pattern, creates the necessary elements and
            recursively calls _handleInline (via. _inlineRecurse)

        @param line: the text to be processed
        @param pattern: the pattern to be checked

        @returns: the appropriate newly created NanoDom element if the
                  pattern matches, None otherwise.
        """

        # match the line to pattern's pre-compiled reg exp.
        # if no match, move on.

        m = pattern.getCompiledRegExp().match(line)
        if not m :
            return None

        # if we got a match let the pattern make us a NanoDom node
        # if it doesn't, move on
        node = pattern.handleMatch(m, self.doc)
        if not node :
            return None

        # determine what we've got to the left and to the right

        left = m.group(1)      # the first match group
        left_list = self._handleInline(left)
        right = m.groups()[-1] # the last match group
        right_list = self._handleInline(right)

        # put the three parts together
        left_list.append(node)
        left_list.extend(right_list)

        return left_list


    def __str__(self):
        """Return the document in XHTML format.

        @returns: A serialized XHTML body."""
        #try :
        doc = self._transform()
        xml = doc.toxml()
        #finally:
        #    doc.unlink()

        # Let's stick in all the raw html pieces

        for i in range(self.htmlStash.html_counter) :
            xml = xml.replace("<p>%s\n</p>" % (HTML_PLACEHOLDER % i),
                              self.htmlStash.rawHtmlBlocks[i] + "\n")
            xml = xml.replace(HTML_PLACEHOLDER % i,
                              self.htmlStash.rawHtmlBlocks[i])

        xml = xml.replace(FN_BACKLINK_TEXT, "&#8617;")

        # And return everything but the top level tag
        xml = xml.strip()[23:-7]

        return xml


    toString = __str__


"""
========================= FOOTNOTES =================================

This section adds footnote handling to markdown.  It can be used as
an example for extending python-markdown with relatively complex
functionality.  While in this case the extension is included inside
the module itself, it could just as easily be added from outside the
module.  Not that all markdown classes above are ignorant about
footnotes.  All footnote functionality is provided separately and
then added to the markdown instance at the run time.

Footnote functionality is attached by calling extendMarkdown()
method of FootnoteExtension.  The method also registers the
extension to allow it's state to be reset by a call to reset()
method.
"""

class FootnoteExtension :

    def __init__ (self) :
        self.DEF_RE = re.compile(r'(\ ?\ ?\ ?)\[\^([^\]]*)\]:\s*(.*)')
        self.SHORT_USE_RE = re.compile(r'\[\^([^\]]*)\]', re.M) # [^a]
        self.reset()

    def extendMarkdown(self, md) :

        self.md = md

        # Stateless extensions do not need to be registered
        md.registerExtension(self)

        # Insert a preprocessor before ReferencePreprocessor
        index = md.preprocessors.index(REFERENCE_PREPROCESSOR)
        preprocessor = FootnotePreprocessor(self)
        preprocessor.md = md
        md.preprocessors.insert(index, preprocessor)

        # Insert an inline pattern before ImageReferencePattern
        FOOTNOTE_RE = r'\[\^([^\]]*)\]' # blah blah [^1] blah
        index = md.inlinePatterns.index(IMAGE_REFERENCE_PATTERN)
        md.inlinePatterns.insert(index, FootnotePattern(FOOTNOTE_RE, self))

        # Insert a post-processor that would actually add the footnote div
        md.postprocessors.append(FootnotePostprocessor(self))

    def reset(self) :
        # May be called by Markdown is state reset is desired

        self.footnote_suffix = "-" + str(int(random.random()*1000000000))
        self.used_footnotes={}
        self.footnotes = {}

    def setFootnote(self, id, text) :
        self.footnotes[id] = text

    def makeFootnoteId(self, num) :
        return 'fn%d%s' % (num, self.footnote_suffix)

    def makeFootnoteRefId(self, num) :
        return 'fnr%d%s' % (num, self.footnote_suffix)

    def makeFootnotesDiv (self, doc) :
        """Creates the div with class='footnote' and populates it with
           the text of the footnotes.

           @returns: the footnote div as a dom element """

        if not self.footnotes.keys() :
            return None

        div = doc.createElement("div")
        div.setAttribute('class', 'footnote')
        hr = doc.createElement("hr")
        div.appendChild(hr)
        ol = doc.createElement("ol")
        div.appendChild(ol)

        footnotes = [(self.used_footnotes[id], id)
                     for id in self.footnotes.keys()]
        footnotes.sort()

        for i, id in footnotes :
            li = doc.createElement('li')
            li.setAttribute('id', self.makeFootnoteId(i))

            self.md._processSection(li, self.footnotes[id].split("\n"))

            #li.appendChild(doc.createTextNode(self.footnotes[id]))

            backlink = doc.createElement('a')
            backlink.setAttribute('href', '#' + self.makeFootnoteRefId(i))
            backlink.setAttribute('class', 'footnoteBackLink')
            backlink.setAttribute('title',
                                  'Jump back to footnote %d in the text' % 1)
            backlink.appendChild(doc.createTextNode(FN_BACKLINK_TEXT))

            if li.childNodes :
                node = li.childNodes[-1]
                if node.type == "text" :
                    node = li
                node.appendChild(backlink)

            ol.appendChild(li)

        return div


class FootnotePreprocessor :

    def __init__ (self, footnotes) :
        self.footnotes = footnotes

    def run(self, lines) :

        self.blockGuru = BlockGuru()
        lines = self._handleFootnoteDefinitions (lines)

        # Make a hash of all footnote marks in the text so that we
        # know in what order they are supposed to appear.  (This
        # function call doesn't really substitute anything - it's just
        # a way to get a callback for each occurence.

        text = "\n".join(lines)
        self.footnotes.SHORT_USE_RE.sub(self.recordFootnoteUse, text)

        return text.split("\n")


    def recordFootnoteUse(self, match) :

        id = match.group(1)
        id = id.strip()
        nextNum = len(self.footnotes.used_footnotes.keys()) + 1
        self.footnotes.used_footnotes[id] = nextNum


    def _handleFootnoteDefinitions(self, lines) :
        """Recursively finds all footnote definitions in the lines.

            @param lines: a list of lines of text
            @returns: a string representing the text with footnote
                      definitions removed """

        i, id, footnote = self._findFootnoteDefinition(lines)

        if id :

            plain = lines[:i]

            detabbed, theRest = self.blockGuru.detectTabbed(lines[i+1:])

            self.footnotes.setFootnote(id,
                                       footnote + "\n"
                                       + "\n".join(detabbed))

            more_plain = self._handleFootnoteDefinitions(theRest)
            return plain + [""] + more_plain

        else :
            return lines

    def _findFootnoteDefinition(self, lines) :
        """Finds the first line of a footnote definition.

            @param lines: a list of lines of text
            @returns: the index of the line containing a footnote definition """

        counter = 0
        for line in lines :
            m = self.footnotes.DEF_RE.match(line)
            if m :
                return counter, m.group(2), m.group(3)
            counter += 1
        return counter, None, None


class FootnotePattern (BasePattern) :

    def __init__ (self, pattern, footnotes) :

        BasePattern.__init__(self, pattern)
        self.footnotes = footnotes

    def handleMatch(self, m, doc) :
        sup = doc.createElement('sup')
        a = doc.createElement('a')
        sup.appendChild(a)
        id = m.group(2)
        num = self.footnotes.used_footnotes[id]
        sup.setAttribute('id', self.footnotes.makeFootnoteRefId(num))
        a.setAttribute('href', '#' + self.footnotes.makeFootnoteId(num))
        a.appendChild(doc.createTextNode(str(num)))
        return sup

class FootnotePostprocessor :

    def __init__ (self, footnotes) :
        self.footnotes = footnotes

    def run(self, doc) :
        footnotesDiv = self.footnotes.makeFootnotesDiv(doc)
        if footnotesDiv :
            doc.documentElement.appendChild(footnotesDiv)

# ====================================================================

def markdown(text) :
    message(VERBOSE, "in markdown.py, received text:\n%s" % text)
    return str(Markdown(text))

def markdownWithFootnotes(text):
    message(VERBOSE, "Running markdown with footnotes, "
            + "received text:\n%s" % text)
    md = Markdown()
    footnoteExtension = FootnoteExtension()
    footnoteExtension.extendMarkdown(md)
    md.source = text

    return str(md)

def test_markdown(args):
    """test markdown at the command line.
        in each test, arg 0 is the module name"""
    print "\nTEST 1: no arguments on command line"
    cmd_line(["markdown.py"])
    print "\nTEST 2a: 1 argument on command line: a good option"
    cmd_line(["markdown.py","-footnotes"])
    print "\nTEST 2b: 1 argument on command line: a bad option"
    cmd_line(["markdown.py","-foodnotes"])
    print "\nTEST 3: 1 argument on command line: non-existent input file"
    cmd_line(["markdown.py","junk.txt"])
    print "\nTEST 4: 1 argument on command line: existing input file"
    lines = """
Markdown text with[^1]:

2. **bold text**,
3. *italic text*.

Then more:

    beginning of code block;
    another line of code block.
    
    a second paragraph of code block.

more text to end our file.

[^1]: "italic" means emphasis.
"""
    fid = "markdown-test.txt"
    f1 = open(fid, 'w+')
    f1.write(lines)
    f1.close()
    cmd_line(["markdown.py",fid])
    print "\nTEST 5: 2 arguments on command line: nofootnotes and input file"
    cmd_line(["markdown.py","-nofootnotes", fid])
    print "\nTEST 6: 2 arguments on command line: footnotes and input file"
    cmd_line(["markdown.py","-footnotes", fid])
    print "\nTEST 7: 3 arguments on command line: nofootnotes,inputfile, outputfile"
    fidout = "markdown-test.html"
    cmd_line(["markdown.py","-nofootnotes", fid, fidout])


def get_vars(args):
    """process the command-line args received; return usable variables"""
    #firstly get the variables

    message(VERBOSE, "in get_vars(), args: %s" % args) 

    if len(args) <= 1:
        option, inFile, outFile = (None, None, None)
    elif len(args) >= 4:
        option, inFile, outFile = args[1:4]
    elif len(args) == 3:
        temp1, temp2 = args[1:3]
        if temp1[0] == '-':
            #then we have an option and inFile
            option, inFile, outFile = temp1, temp2, None
        else:
            #we have no option, so we must have inFile and outFile
            option, inFile, outFile = None, temp1, temp2
    else:
        #len(args) = 2
        #we have only one usable arg: might be an option or a file
        temp1 = args[1]
        
        message(VERBOSE, "our single arg is: %s" % str(temp1))

        if temp1[0] == '-':
            #then we have an option 
            option, inFile, outFile = temp1, None, None
        else:
            #we have no option, so we must have inFile
            option, inFile, outFile = None, temp1, None
    
    message(VERBOSE,
            "prior to validation, option: %s, inFile: %s, outFile: %s" %
            (str(option), str(inFile), str(outFile),))
    
    return option, inFile, outFile


USAGE = """
\nUsing markdown.py:

    python markdown.py [option] input_file_with_markdown.txt [output_file.html]

Options:

    -footnotes or -fn   : generate markdown with footnotes
    -test or -t         : run a self-test
    -help or -h         : print this message

"""
    
VALID_OPTIONS = ['footnotes','nofootnotes', 'fn', 'test', 't', 'f',
                 'help', 'h']

EXPANDED_OPTIONS =  { "fn" : "footnotes",
                      "t"  : "test",
                      "h"  : "help" }


def validate_option(option) :

    """ Check if the option makes sense and print an appropriate message
        if it isn't.
        
        @return: valid option string or None
    """

    #now validate the variables
    if (option is not None):
        if (len(option) > 1 and option[1:] in VALID_OPTIONS) :
            option = option[1:]

            if option in EXPANDED_OPTIONS.keys() :
                option = EXPANDED_OPTIONS[option]
            return option
        else:
            message(CRITICAL,
                    "\nSorry, I don't understand option %s" % option)
            message(CRITICAL, USAGE)
            return None


def validate_input_file(inFile) :        
    """ Check if the input file is specified and exists.

        @return: valid input file path or None
    """

    if not inFile :
        message(CRITICAL,
                "\nI need an input filename.\n")
        message(CRITICAL, USAGE)
        return None
    
        
    if os.access(inFile, os.R_OK):
        return inFile
    else :
        message(CRITICAL, "Sorry, I can't find input file %s" % str(inFile))
        return None

    
            

def cmd_line(args):

    message(VERBOSE, "in cmd_line with args: %s" % args)

    option, inFile, outFile = get_vars(args)

    if option :
        option = validate_option(option)
        if not option : return

    if option == "help" :
        message(CRITICAL, USAGE)
        return
    elif option == "test" :
        test_markdown(None)
        return

    inFile = validate_input_file(inFile)
    if not inFile :
        return
    else :
        input = file(inFile).read()

    message(VERBOSE, "Validated command line parameters:" +             
             "\n\toption: %s, \n\tinFile: %s, \n\toutFile: %s" % (
             str(option), str(inFile), str(outFile),))

    if option == "footnotes" :
        md_function = markdownWithFootnotes
    else :
        md_function = markdown

    if outFile is None:
        print md_function(input)
    else:
        output = md_function(input)
        f1 = open(outFile, "w+")
        f1.write(output)
        f1.close()
        
        if os.access(outFile, os.F_OK):
            message(INFO, "Successfully wrote %s" % outFile)
        else:
            message(INFO, "Failed to write %s" % outFile)


if __name__ == '__main__':
    """ Run Markdown from the command line.
        Set debug = 3 at top of file to get diagnostic output"""
    args = sys.argv
        
    #set testing=1 to test the command-line response of markdown.py
    testing = 0
    if testing:
        test_markdown(args)
    else:
        cmd_line(args)

"""
CHANGELOG
=========

Feb. 28, 2006: Clean-up and command-line handling by Stewart
Midwinter. (Version 1.3)

Feb. 24, 2006: Fixed a bug with the last line of the list appearing
again as a separate paragraph.  Incorporated Chris Clark's "mailto"
patch.  Added support for <br /> at the end of lines ending in two or
more spaces.  Fixed a crashing bug when using ImageReferencePattern.
Added several utility methods to Nanodom.  (Version 1.2)

Jan. 31, 2006: Added "hr" and "hr/" to BLOCK_LEVEL_ELEMENTS and
changed <hr/> to <hr />.  (Thanks to Sergej Chodarev.)

Nov. 26, 2005: Fixed a bug with certain tabbed lines inside lists
getting wrapped in <pre><code>.  (v. 1.1)

Nov. 19, 2005: Made "<!...", "<?...", etc. behave like block-level
HTML tags.

Nov. 14, 2005: Added entity code and email autolink fix by Tiago
Cogumbreiro.  Fixed some small issues with backticks to get 100%
compliance with John's test suite.  (v. 1.0)

Nov. 7, 2005: Added an unlink method for documents to aid with memory
collection (per Doug Sauder's suggestion).

Oct. 29, 2005: Restricted a set of html tags that get treated as
block-level elements.

Sept. 18, 2005: Refactored the whole script to make it easier to
customize it and made footnote functionality into an extension.
(v. 0.9)

Sept. 5, 2005: Fixed a bug with multi-paragraph footnotes.  Added
attribute support.

Sept. 1, 2005: Changed the way headers are handled to allow inline
syntax in headers (e.g. links) and got the lists to use p-tags
correctly (v. 0.8)

Aug. 29, 2005: Added flexible tabs, fixed a few small issues, added
basic support for footnotes.  Got rid of xml.dom.minidom and added
pretty-printing. (v. 0.7)

Aug. 13, 2005: Fixed a number of small bugs in order to conform to the
test suite.  (v. 0.6)

Aug. 11, 2005: Added support for inline html and entities, inline
images, autolinks, underscore emphasis. Cleaned up and refactored the
code, added some more comments.

Feb. 19, 2005: Rewrote the handling of high-level elements to allow
multi-line list items and all sorts of nesting.

Feb. 3, 2005: Reference-style links, single-line lists, backticks,
escape, emphasis in the beginning of the paragraph.

Nov. 2004: Added links, blockquotes, html blocks to Manfred
Stienstra's code

Apr. 2004: Manfred's version at http://www.dwerg.net/projects/markdown/

"""






