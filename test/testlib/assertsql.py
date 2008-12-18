
from sqlalchemy.interfaces import ConnectionProxy
from sqlalchemy.engine.default import DefaultDialect
from sqlalchemy.engine.base import Connection
from sqlalchemy import util
import testing
import re

class AssertRule(object):
    def process_execute(self, clauseelement, *multiparams, **params):
        pass

    def process_cursor_execute(self, statement, parameters, context, executemany):
        pass
        
    def is_consumed(self):
        """Return True if this rule has been consumed, False if not.
        
        Should raise an AssertionError if this rule's condition has definitely failed.
        
        """
        raise NotImplementedError()
    
    def rule_passed(self):
        """Return True if the last test of this rule passed, False if failed, None if no test was applied."""
        
        raise NotImplementedError()
        
    def consume_final(self):
        """Return True if this rule has been consumed.
        
        Should raise an AssertionError if this rule's condition has not been consumed or has failed.
        
        """
        
        if self._result is None:
            assert False, "Rule has not been consumed"
            
        return self.is_consumed()

class SQLMatchRule(AssertRule):
    def __init__(self):
        self._result = None
        self._errmsg = ""
    
    def rule_passed(self):
        return self._result
        
    def is_consumed(self):
        if self._result is None:
            return False
            
        assert self._result, self._errmsg
        
        return True
    
class ExactSQL(SQLMatchRule):
    def __init__(self, sql, params=None):
        SQLMatchRule.__init__(self)
        self.sql = sql
        self.params = params
    
    def process_cursor_execute(self, statement, parameters, context, executemany):
        if not context:
            return
            
        _received_statement = _process_engine_statement(statement, context)
        _received_parameters = context.compiled_parameters
        
        # TODO: remove this step once all unit tests
        # are migrated, as ExactSQL should really be *exact* SQL 
        sql = _process_assertion_statement(self.sql, context)
        
        equivalent = _received_statement == sql
        if self.params:
            if util.callable(self.params):
                params = self.params(context)
            else:
                params = self.params

            if not isinstance(params, list):
                params = [params]
            equivalent = equivalent and params == context.compiled_parameters
        else:
            params = {}
        
        
        self._result = equivalent
        if not self._result:
            self._errmsg = "Testing for exact statement %r exact params %r, " \
                "received %r with params %r" % (sql, params, _received_statement, _received_parameters)
    

class RegexSQL(SQLMatchRule):
    def __init__(self, regex, params=None):
        SQLMatchRule.__init__(self)
        self.regex = re.compile(regex)
        self.orig_regex = regex
        self.params = params

    def process_cursor_execute(self, statement, parameters, context, executemany):
        if not context:
            return

        _received_statement = _process_engine_statement(statement, context)
        _received_parameters = context.compiled_parameters

        equivalent = bool(self.regex.match(_received_statement))
        if self.params:
            if util.callable(self.params):
                params = self.params(context)
            else:
                params = self.params

            if not isinstance(params, list):
                params = [params]
            
            # do a positive compare only
            for param, received in zip(params, _received_parameters):
                for k, v in param.iteritems():
                    if k not in received or received[k] != v:
                        equivalent = False
                        break
        else:
            params = {}

        self._result = equivalent
        if not self._result:
            self._errmsg = "Testing for regex %r partial params %r, "\
                "received %r with params %r" % (self.orig_regex, params, _received_statement, _received_parameters)

class CompiledSQL(SQLMatchRule):
    def __init__(self, statement, params):
        SQLMatchRule.__init__(self)
        self.statement = statement
        self.params = params

    def process_cursor_execute(self, statement, parameters, context, executemany):
        if not context:
            return

        _received_parameters = context.compiled_parameters
        
        # recompile from the context, using the default dialect
        compiled = context.compiled.statement.\
                compile(dialect=DefaultDialect(), column_keys=context.compiled.column_keys)
                
        _received_statement = re.sub(r'\n', '', str(compiled))
        
        equivalent = self.statement == _received_statement
        if self.params:
            if util.callable(self.params):
                params = self.params(context)
            else:
                params = self.params

            if not isinstance(params, list):
                params = [params]
            
            # do a positive compare only
            for param, received in zip(params, _received_parameters):
                for k, v in param.iteritems():
                    if k not in received or received[k] != v:
                        equivalent = False
                        break
        else:
            params = {}

        self._result = equivalent
        if not self._result:
            self._errmsg = "Testing for compiled statement %r partial params %r, " \
                    "received %r with params %r" % (self.statement, params, _received_statement, _received_parameters)
    
        
class CountStatements(AssertRule):
    def __init__(self, count):
        self.count = count
        self._statement_count = 0
        
    def process_execute(self, clauseelement, *multiparams, **params):
        self._statement_count += 1

    def process_cursor_execute(self, statement, parameters, context, executemany):
        pass

    def is_consumed(self):
        return False
    
    def consume_final(self):
        assert self.count == self._statement_count, "desired statement count %d does not match %d" % (self.count, self._statement_count)
        return True
        
class AllOf(AssertRule):
    def __init__(self, *rules):
        self.rules = set(rules)
        
    def process_execute(self, clauseelement, *multiparams, **params):
        for rule in self.rules:
            rule.process_execute(clauseelement, *multiparams, **params)

    def process_cursor_execute(self, statement, parameters, context, executemany):
        for rule in self.rules:
            rule.process_cursor_execute(statement, parameters, context, executemany)

    def is_consumed(self):
        if not self.rules:
            return True
        
        for rule in list(self.rules):
            if rule.rule_passed(): # a rule passed, move on
                self.rules.remove(rule)
                return len(self.rules) == 0

        assert False, "No assertion rules were satisfied for statement"
    
    def consume_final(self):
        return len(self.rules) == 0
        
def _process_engine_statement(query, context):
    if context.engine.name == 'mssql' and query.endswith('; select scope_identity()'):
        query = query[:-25]
    
    query = re.sub(r'\n', '', query)
    
    return query
    
def _process_assertion_statement(query, context):
    paramstyle = context.dialect.paramstyle
    if paramstyle == 'named':
        pass
    elif paramstyle =='pyformat':
        query = re.sub(r':([\w_]+)', r"%(\1)s", query)
    else:
        # positional params
        repl = None
        if paramstyle=='qmark':
            repl = "?"
        elif paramstyle=='format':
            repl = r"%s"
        elif paramstyle=='numeric':
            repl = None
        query = re.sub(r':([\w_]+)', repl, query)

    return query

class SQLAssert(ConnectionProxy):
    rules = None
    
    def add_rules(self, rules):
        self.rules = list(rules)
    
    def statement_complete(self):
        for rule in self.rules:
            if not rule.consume_final():
                assert False, "All statements are complete, but pending assertion rules remain"

    def clear_rules(self):
        del self.rules
        
    def execute(self, conn, execute, clauseelement, *multiparams, **params):
        result = execute(clauseelement, *multiparams, **params)

        if self.rules is not None:
            if not self.rules:
                assert False, "All rules have been exhausted, but further statements remain"
            rule = self.rules[0]
            rule.process_execute(clauseelement, *multiparams, **params)
            if rule.is_consumed():
                self.rules.pop(0)
            
        return result
        
    def cursor_execute(self, execute, cursor, statement, parameters, context, executemany):
        result = execute(cursor, statement, parameters, context)
        
        if self.rules:
            rule = self.rules[0]
            rule.process_cursor_execute(statement, parameters, context, executemany)

        return result

asserter = SQLAssert()
    
