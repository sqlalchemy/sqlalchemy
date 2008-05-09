"""A collection of Python source transformers.

Supports the 'clone' command, providing source code transforms to run the test
suite on pre Python 2.4-level parser implementations.

Includes::

  py23
     Converts 2.4-level source code into 2.3-parsable source.
     Currently only rewrites @decorators, but generator transformations
     are possible.
  py23_decorators
     py23 is currently an alias for py23_decorators.
"""

import sys
from tokenize import generate_tokens, INDENT, DEDENT, NAME, OP, NL, NEWLINE, \
     NUMBER, STRING, COMMENT

__all__ = ['py23_decorators', 'py23']


def py23_decorators(lines):
    """Translates @decorators in source lines to 2.3 syntax."""

    tokens = peekable(generate_tokens(iter(lines).next))
    text = untokenize(backport_decorators(tokens))
    return [x + '\n' for x in text.split('\n')]

py23 = py23_decorators


def backport_decorators(stream):
    """Restates @decorators in 2.3 syntax

    Operates on token streams. Converts::

      @foo
      @bar(1, 2)
      def quux():
          pass
    into::

      def quux():
          pass
      quux = bar(1, 2)(quux)
      quux = foo(quux)

    Fails on decorated one-liners::

      @decorator
      def fn(): pass
    """

    if not hasattr(stream, 'peek'):
        stream = peekable(iter(stream))

    stack = [_DecoratorState('')]
    emit = []
    for ttype, tok, _, _, _ in stream:
        current = stack[-1]
        if ttype == INDENT:
            current = _DecoratorState(tok)
            stack.append(current)
        elif ttype == DEDENT:
            previous = stack.pop()
            assert not previous.decorations
            current = stack[-1]
            if current.decorations:
                ws = pop_trailing_whitespace(emit)

                emit.append((ttype, tok))
                for decorator, misc in reversed(current.decorations):
                    if not decorator or decorator[0][1] != '@':
                        emit.extend(decorator)
                    else:
                        emit.extend(
                            [(NAME, current.fn_name), (OP, '=')] +
                            decorator[1:] +
                            [(OP, '('), (NAME, current.fn_name), (OP, ')')])
                    emit.extend(misc)
                current.decorations = []
                emit.extend(ws)
                continue
        elif ttype == OP and tok == '@':
            current.in_decorator = True
            decoration = [(ttype, tok)]
            current.decorations.append((decoration, []))
            current.consume_identifier(stream)
            if stream.peek()[1] == '(':
                current.consume_parened(stream)
            continue
        elif ttype == NAME and tok == 'def':
            current.in_decorator = False
            current.fn_name = stream.peek()[1]
        elif current.in_decorator:
            current.append_misc((ttype, tok))
            continue

        emit.append((ttype, tok))
    return emit

class _DecoratorState(object):
    """Holds state for restating decorators as function calls."""

    in_decorator = False
    fn_name = None
    def __init__(self, indent):
        self.indent = indent
        self.decorations = []
    def append_misc(self, token):
        if not self.decorations:
            self.decorations.append(([], []))
        self.decorations[-1][1].append(token)
    def consume_identifier(self, stream):
        while True:
            typ, value = stream.peek()[:2]
            if not (typ == NAME or (typ == OP and value == '.')):
                break
            self.decorations[-1][0].append(stream.next()[:2])
    def consume_parened(self, stream):
        """Consume a (paren) sequence from a token seq starting with ("""
        depth, offsets = 0, {'(':1, ')':-1}
        while True:
            typ, value = stream.next()[:2]
            if typ == OP:
                depth += offsets.get(value, 0)
            self.decorations[-1][0].append((typ, value))
            if depth == 0:
                break

def pop_trailing_whitespace(tokens):
    """Removes trailing whitespace tokens from a token list."""

    popped = []
    for token in reversed(list(tokens)):
        if token[0] not in (NL, COMMENT):
            break
        popped.append(tokens.pop())
    return popped

def untokenize(iterable):
    """Turns a stream of tokens into a Python source str.

    A PEP-8-ish variant of Python 2.5+'s tokenize.untokenize.  Produces output
    that's not perfect, but is at least readable.  The stdlib version is
    basically unusable.
    """

    if not hasattr(iterable, 'peek'):
        iterable = peekable(iter(iterable))

    startline = False
    indents = []
    toks = []
    toks_append = toks.append

    # this is pretty roughly hacked.  i think it could get very close to
    # perfect by rewriting to operate over a sliding window of
    # (prev, current, next) token sets + making some grouping macros to
    # include all the tokens and operators this omits.
    for tok in iterable:
        toknum, tokval = tok[:2]

        try:
            next_num, next_val = iterable.peek()[:2]
        except StopIteration:
            next_num, next_val = None, None

        if toknum == NAME:
            if tokval == 'in':
                tokval += ' '
            elif next_num == OP:
                if next_val not in ('(', ')', '[', ']', '{', '}',
                                      ':', '.', ',',):
                    tokval += ' '
            elif next_num != NEWLINE:
                tokval += ' '
        elif toknum == OP:
            if tokval in ('(', '@', '.', '[', '{', '*', '**'):
                pass
            elif tokval in ('%', ':') and next_num not in (NEWLINE, ):
                tokval += ' '
            elif next_num in (NAME, COMMENT,
                              NUMBER, STRING):
                tokval += ' '
            elif (tokval in (')', ']', '}') and next_num == OP and
                  '=' in next_val):
                tokval += ' '
            elif tokval == ',' or '=' in tokval:
                tokval += ' '
        elif toknum in (NUMBER, STRING):
            if next_num == OP and next_val not in (')', ']', '}', ',', ':'):
                tokval += ' '
            elif next_num == NAME:
                tokval += ' '

        # would be nice to indent continued lines...
        if toknum == INDENT:
            indents.append(tokval)
            continue
        elif toknum == DEDENT:
            indents.pop()
            continue
        elif toknum in (NEWLINE, COMMENT, NL):
            startline = True
        elif startline and indents:
            toks_append(indents[-1])
            startline = False
        toks_append(tokval)
    return ''.join(toks)


class peekable(object):
    """A iterator wrapper that allows peek()ing at the next value."""

    def __init__(self, iterator):
        self.iterator = iterator
        self.buffer = []
    def next(self):
        if self.buffer:
            return self.buffer.pop(0)
        return self.iterator.next()
    def peek(self):
        if self.buffer:
            return self.buffer[0]
        x = self.iterator.next()
        self.buffer.append(x)
        return x
    def __iter__(self):
        return self

if __name__ == '__main__':
    # runnable.  converts a named file to 2.3.
    input = open(len(sys.argv) == 2 and sys.argv[1] or __file__)

    tokens = generate_tokens(input.readline)
    back = backport_decorators(tokens)
    print untokenize(back)
