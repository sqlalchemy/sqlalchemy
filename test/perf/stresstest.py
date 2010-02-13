import gc
import sys
import timeit
import cProfile

from sqlalchemy import MetaData, Table, Column
from sqlalchemy.types import *
from sqlalchemy.orm import mapper, clear_mappers

metadata = MetaData()

def gen_table(num_fields, field_type, metadata):
    return Table('test', metadata,
        Column('id', Integer, primary_key=True),
        *[Column("field%d" % fnum, field_type)
          for fnum in range(num_fields)])

def insert(test_table, num_fields, num_records, genvalue, verbose=True):
    if verbose:
        print "building insert values...",
        sys.stdout.flush()
    values = [dict(("field%d" % fnum, genvalue(rnum, fnum))
                   for fnum in range(num_fields))
              for rnum in range(num_records)]
    if verbose:
        print "inserting...",
        sys.stdout.flush()
    def db_insert():
        test_table.insert().execute(values)
    sys.modules['__main__'].db_insert = db_insert
    timing = timeit.timeit("db_insert()",
                            "from __main__ import db_insert",
                            number=1)
    if verbose:
        print "%s" % round(timing, 3)

def check_result(results, num_fields, genvalue, verbose=True):
    if verbose:
        print "checking...",
        sys.stdout.flush()
    for rnum, row in enumerate(results):
        expected = tuple([rnum + 1] +
                         [genvalue(rnum, fnum) for fnum in range(num_fields)])
        assert row == expected, "got: %s\nexpected: %s" % (row, expected)
    return True

def avgdev(values, comparison):
    return sum(value - comparison for value in values) / len(values)

def nicer_res(values, printvalues=False):
    if printvalues:
        print values
    min_time = min(values)
    return round(min_time, 3), round(avgdev(values, min_time), 2)

def profile_func(func_name, verbose=True):
    if verbose:
        print "profiling...",
        sys.stdout.flush()
    cProfile.run('%s()' % func_name, 'prof')

def time_func(func_name, num_tests=1, verbose=True):
    if verbose:
        print "timing...",
        sys.stdout.flush()
    timings = timeit.repeat('%s()' % func_name,
                            "from __main__ import %s" % func_name,
                            number=num_tests, repeat=5)
    avg, dev = nicer_res(timings)
    if verbose:
        print "%s (%s)" % (avg, dev)
    else:
        print avg

def profile_and_time(func_name, num_tests=1):
    profile_func(func_name)
    time_func(func_name, num_tests)

def iter_results(raw_results):
    return [tuple(row) for row in raw_results]

def getattr_results(raw_results):
    return [
        (r.id,
         r.field0, r.field1, r.field2, r.field3, r.field4,
         r.field5, r.field6, r.field7, r.field8, r.field9)
         for r in raw_results]

def fetchall(test_table):
    def results():
        return test_table.select().order_by(test_table.c.id).execute() \
                         .fetchall()
    return results

def hashable_set(l):
    hashables = []
    for o in l:
        try:
            hash(o)
            hashables.append(o)
        except:
            pass
    return set(hashables)

def prepare(field_type, genvalue, engineurl='sqlite://',
            num_fields=10, num_records=1000, freshdata=True, verbose=True):
    global metadata
    metadata.clear()
    metadata.bind = engineurl
    test_table = gen_table(num_fields, field_type, metadata)
    if freshdata:
        metadata.drop_all()
        metadata.create_all()
        insert(test_table, num_fields, num_records, genvalue, verbose)
    return test_table

def time_dbfunc(test_table, test_func, genvalue,
                class_=None,
                getresults_func=None,
                num_fields=10, num_records=1000, num_tests=1,
                check_results=check_result, profile=True,
                check_leaks=True, print_leaks=False, verbose=True):
    if verbose:
        print "testing '%s'..." % test_func.__name__,
    sys.stdout.flush()
    if class_ is not None:
        clear_mappers()
        mapper(class_, test_table)
    if getresults_func is None:
        getresults_func = fetchall(test_table)
    def test():
        return test_func(getresults_func())
    sys.modules['__main__'].test = test
    if check_leaks:
        gc.collect()
        objects_before = gc.get_objects()
        num_objects_before = len(objects_before)
        hashable_objects_before = hashable_set(objects_before)
#    gc.set_debug(gc.DEBUG_LEAK)
    if check_results:
        check_results(test(), num_fields, genvalue, verbose)
    if check_leaks:
        gc.collect()
        objects_after = gc.get_objects()
        num_objects_after = len(objects_after)
        num_leaks = num_objects_after - num_objects_before
        hashable_objects_after = hashable_set(objects_after)
        diff = hashable_objects_after - hashable_objects_before
        ldiff = len(diff)
        if print_leaks and ldiff < num_records:
            print "\n*** hashable objects leaked (%d) ***" % ldiff
            print '\n'.join(map(str, diff))
            print "***\n"

        if num_leaks > num_records:
            print "(leaked: %d !)" % num_leaks,
    if profile:
        profile_func('test', verbose)
    time_func('test', num_tests, verbose)

def profile_and_time_dbfunc(test_func, field_type, genvalue,
                            class_=None,
                            getresults_func=None,
                            engineurl='sqlite://', freshdata=True,
                            num_fields=10, num_records=1000, num_tests=1,
                            check_results=check_result, profile=True,
                            check_leaks=True, print_leaks=False, verbose=True):
    test_table = prepare(field_type, genvalue, engineurl,
                         num_fields, num_records, freshdata, verbose)
    time_dbfunc(test_table, test_func, genvalue, class_,
                getresults_func,
                num_fields, num_records, num_tests,
                check_results, profile,
                check_leaks, print_leaks, verbose)
