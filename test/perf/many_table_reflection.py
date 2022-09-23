from argparse import ArgumentDefaultsHelpFormatter
from argparse import ArgumentParser
from collections import defaultdict
from contextlib import contextmanager
from functools import wraps
from pprint import pprint
import random
import time

import sqlalchemy as sa
from sqlalchemy.engine import Inspector

types = (sa.Integer, sa.BigInteger, sa.String(200), sa.DateTime)
USE_CONNECTION = False


def generate_table(meta: sa.MetaData, min_cols, max_cols, dialect_name):
    col_number = random.randint(min_cols, max_cols)
    table_num = len(meta.tables)
    add_identity = random.random() > 0.90
    identity = sa.Identity(
        always=random.randint(0, 1),
        start=random.randint(1, 100),
        increment=random.randint(1, 7),
    )
    is_mssql = dialect_name == "mssql"
    cols = []
    for i in range(col_number - (0 if is_mssql else add_identity)):
        args = []
        if random.random() < 0.95 or table_num == 0:
            if is_mssql and add_identity and i == 0:
                args.append(sa.Integer)
                args.append(identity)
            else:
                args.append(random.choice(types))
        else:
            args.append(
                sa.ForeignKey(f"table_{table_num-1}.table_{table_num-1}_col_1")
            )
        cols.append(
            sa.Column(
                f"table_{table_num}_col_{i+1}",
                *args,
                primary_key=i == 0,
                comment=f"primary key of table_{table_num}"
                if i == 0
                else None,
                index=random.random() > 0.9 and i > 0,
                unique=random.random() > 0.95 and i > 0,
            )
        )
    if add_identity and not is_mssql:
        cols.append(
            sa.Column(
                f"table_{table_num}_col_{col_number}",
                sa.Integer,
                identity,
            )
        )
    args = ()
    if table_num % 3 == 0:
        # mysql can't do check constraint on PK col
        args = (sa.CheckConstraint(cols[1].is_not(None)),)
    return sa.Table(
        f"table_{table_num}",
        meta,
        *cols,
        *args,
        comment=f"comment for table_{table_num}" if table_num % 2 else None,
    )


def generate_meta(schema_name, table_number, min_cols, max_cols, dialect_name):
    meta = sa.MetaData(schema=schema_name)
    log = defaultdict(int)
    for _ in range(table_number):
        t = generate_table(meta, min_cols, max_cols, dialect_name)
        log["tables"] += 1
        log["columns"] += len(t.columns)
        log["index"] += len(t.indexes)
        log["check_con"] += len(
            [c for c in t.constraints if isinstance(c, sa.CheckConstraint)]
        )
        log["foreign_keys_con"] += len(
            [
                c
                for c in t.constraints
                if isinstance(c, sa.ForeignKeyConstraint)
            ]
        )
        log["unique_con"] += len(
            [c for c in t.constraints if isinstance(c, sa.UniqueConstraint)]
        )
        log["identity"] += len([c for c in t.columns if c.identity])

    print("Meta info", dict(log))
    return meta


def log(fn):
    @wraps(fn)
    def wrap(*a, **kw):
        print("Running", fn.__name__, "...", flush=True, end="")
        try:
            r = fn(*a, **kw)
        except NotImplementedError:
            print(" [not implemented]", flush=True)
            r = None
        else:
            print("... done", flush=True)
        return r

    return wrap


tests = {}


def define_test(fn):
    name: str = fn.__name__
    if name.startswith("reflect_"):
        name = name[8:]
    tests[name] = wfn = log(fn)
    return wfn


@log
def create_tables(engine, meta):
    tables = list(meta.tables.values())
    for i in range(0, len(tables), 500):
        meta.create_all(engine, tables[i : i + 500])


@log
def drop_tables(engine, meta, schema_name, table_names: list):
    tables = list(meta.tables.values())[::-1]
    for i in range(0, len(tables), 500):
        meta.drop_all(engine, tables[i : i + 500])

    remaining = sa.inspect(engine).get_table_names(schema=schema_name)
    suffix = ""
    if engine.dialect.name.startswith("postgres"):
        suffix = "CASCADE"

    remaining = sorted(
        remaining, key=lambda tn: int(tn.partition("_")[2]), reverse=True
    )
    with engine.connect() as conn:
        for i, tn in enumerate(remaining):
            if engine.dialect.requires_name_normalize:
                name = engine.dialect.denormalize_name(tn)
            else:
                name = tn
            if schema_name:
                conn.execute(
                    sa.schema.DDL(
                        f'DROP TABLE {schema_name}."{name}" {suffix}'
                    )
                )
            else:
                conn.execute(sa.schema.DDL(f'DROP TABLE "{name}" {suffix}'))
            if i % 500 == 0:
                conn.commit()
        conn.commit()


@log
def reflect_tables(engine, schema_name):
    ref_meta = sa.MetaData(schema=schema_name)
    ref_meta.reflect(engine)


def verify_dict(multi, single, str_compare=False):
    if single is None or multi is None:
        return
    if single != multi:
        keys = set(single) | set(multi)
        diff = []
        for key in sorted(keys):
            se, me = single.get(key), multi.get(key)
            if str(se) != str(me) if str_compare else se != me:
                diff.append((key, single.get(key), multi.get(key)))
        if diff:
            print("\nfound different result:")
            pprint(diff)


def _single_test(
    singe_fn_name,
    multi_fn_name,
    engine,
    schema_name,
    table_names,
    timing,
    mode,
):
    single = None
    if "single" in mode:
        singe_fn = getattr(Inspector, singe_fn_name)

        def go(bind):
            insp = sa.inspect(bind)
            single = {}
            with timing(singe_fn.__name__):
                for t in table_names:
                    single[(schema_name, t)] = singe_fn(
                        insp, t, schema=schema_name
                    )
            return single

        if USE_CONNECTION:
            with engine.connect() as c:
                single = go(c)
        else:
            single = go(engine)

    multi = None
    if "multi" in mode:
        insp = sa.inspect(engine)
        multi_fn = getattr(Inspector, multi_fn_name)
        with timing(multi_fn.__name__):
            multi = multi_fn(insp, schema=schema_name)
    return (multi, single)


@define_test
def reflect_columns(
    engine, schema_name, table_names, timing, mode, ignore_diff
):
    multi, single = _single_test(
        "get_columns",
        "get_multi_columns",
        engine,
        schema_name,
        table_names,
        timing,
        mode,
    )
    if not ignore_diff:
        verify_dict(multi, single, str_compare=True)


@define_test
def reflect_table_options(
    engine, schema_name, table_names, timing, mode, ignore_diff
):
    multi, single = _single_test(
        "get_table_options",
        "get_multi_table_options",
        engine,
        schema_name,
        table_names,
        timing,
        mode,
    )
    if not ignore_diff:
        verify_dict(multi, single)


@define_test
def reflect_pk(engine, schema_name, table_names, timing, mode, ignore_diff):
    multi, single = _single_test(
        "get_pk_constraint",
        "get_multi_pk_constraint",
        engine,
        schema_name,
        table_names,
        timing,
        mode,
    )
    if not ignore_diff:
        verify_dict(multi, single)


@define_test
def reflect_comment(
    engine, schema_name, table_names, timing, mode, ignore_diff
):
    multi, single = _single_test(
        "get_table_comment",
        "get_multi_table_comment",
        engine,
        schema_name,
        table_names,
        timing,
        mode,
    )
    if not ignore_diff:
        verify_dict(multi, single)


@define_test
def reflect_whole_tables(
    engine, schema_name, table_names, timing, mode, ignore_diff
):
    single = None
    meta = sa.MetaData(schema=schema_name)

    if "single" in mode:

        def go(bind):
            single = {}
            with timing("Table_autoload_with"):
                for name in table_names:
                    single[(None, name)] = sa.Table(
                        name, meta, autoload_with=bind
                    )
            return single

        if USE_CONNECTION:
            with engine.connect() as c:
                single = go(c)
        else:
            single = go(engine)

    multi_meta = sa.MetaData(schema=schema_name)
    if "multi" in mode:
        with timing("MetaData_reflect"):
            multi_meta.reflect(engine, only=table_names)
    return (multi_meta, single)


@define_test
def reflect_check_constraints(
    engine, schema_name, table_names, timing, mode, ignore_diff
):
    multi, single = _single_test(
        "get_check_constraints",
        "get_multi_check_constraints",
        engine,
        schema_name,
        table_names,
        timing,
        mode,
    )
    if not ignore_diff:
        verify_dict(multi, single)


@define_test
def reflect_indexes(
    engine, schema_name, table_names, timing, mode, ignore_diff
):
    multi, single = _single_test(
        "get_indexes",
        "get_multi_indexes",
        engine,
        schema_name,
        table_names,
        timing,
        mode,
    )
    if not ignore_diff:
        verify_dict(multi, single)


@define_test
def reflect_foreign_keys(
    engine, schema_name, table_names, timing, mode, ignore_diff
):
    multi, single = _single_test(
        "get_foreign_keys",
        "get_multi_foreign_keys",
        engine,
        schema_name,
        table_names,
        timing,
        mode,
    )
    if not ignore_diff:
        verify_dict(multi, single)


@define_test
def reflect_unique_constraints(
    engine, schema_name, table_names, timing, mode, ignore_diff
):
    multi, single = _single_test(
        "get_unique_constraints",
        "get_multi_unique_constraints",
        engine,
        schema_name,
        table_names,
        timing,
        mode,
    )
    if not ignore_diff:
        verify_dict(multi, single)


def _apply_events(engine):
    queries = defaultdict(list)

    now = 0

    @sa.event.listens_for(engine, "before_cursor_execute")
    def before_cursor_execute(
        conn, cursor, statement, parameters, context, executemany
    ):

        nonlocal now
        now = time.time()

    @sa.event.listens_for(engine, "after_cursor_execute")
    def after_cursor_execute(
        conn, cursor, statement, parameters, context, executemany
    ):
        total = time.time() - now

        if context and context.compiled:
            statement_str = context.compiled.string
        else:
            statement_str = statement
        queries[statement_str].append(total)

    return queries


def _print_query_stats(queries):
    number_of_queries = sum(
        len(query_times) for query_times in queries.values()
    )
    print("-" * 50)
    q_list = list(queries.items())
    q_list.sort(key=lambda rec: -sum(rec[1]))
    total = sum([sum(t) for _, t in q_list])
    print(f"total number of queries: {number_of_queries}. Total time {total}")
    print("-" * 50)

    for stmt, times in q_list:
        total_t = sum(times)
        max_t = max(times)
        min_t = min(times)
        avg_t = total_t / len(times)
        times.sort()
        median_t = times[len(times) // 2]

        print(
            f"Query times: {total_t=}, {max_t=}, {min_t=}, {avg_t=}, "
            f"{median_t=}  Number of calls: {len(times)}"
        )
        print(stmt.strip(), "\n")


def main(db, schema_name, table_number, min_cols, max_cols, args):
    timing = timer()
    if args.pool_class:
        engine = sa.create_engine(
            db,
            echo=args.echo,
            poolclass=getattr(sa.pool, args.pool_class),
            future=True,
        )
    else:
        engine = sa.create_engine(db, echo=args.echo, future=True)

    if engine.name == "oracle":
        # clear out oracle caches so that we get the real-world time the
        # queries would normally take for scripts that aren't run repeatedly
        with engine.connect() as conn:
            # https://stackoverflow.com/questions/2147456/how-to-clear-all-cached-items-in-oracle
            conn.exec_driver_sql("alter system flush buffer_cache")
            conn.exec_driver_sql("alter system flush shared_pool")
    if not args.no_create:
        print(
            f"Generating {table_number} using engine {engine} in "
            f"schema {schema_name or 'default'}",
        )
    meta = sa.MetaData()
    table_names = []
    stats = {}
    try:
        if not args.no_create:
            with timing("populate-meta"):
                meta = generate_meta(
                    schema_name, table_number, min_cols, max_cols, engine.name
                )
            with timing("create-tables"):
                create_tables(engine, meta)

        with timing("get_table_names"):
            with engine.connect() as conn:
                table_names = engine.dialect.get_table_names(
                    conn, schema=schema_name
                )
        print(
            f"Reflected table number {len(table_names)} in "
            f"schema {schema_name or 'default'}"
        )
        mode = {"single", "multi"}
        if args.multi_only:
            mode.discard("single")
        if args.single_only:
            mode.discard("multi")

        if args.sqlstats:
            print("starting stats for subsequent tests")
            stats = _apply_events(engine)
        for test_name, test_fn in tests.items():
            if test_name in args.test or "all" in args.test:
                test_fn(
                    engine,
                    schema_name,
                    table_names,
                    timing,
                    mode,
                    args.ignore_diff,
                )

        if args.reflect:
            with timing("reflect-tables"):
                reflect_tables(engine, schema_name)
    finally:
        # copy stats to new dict
        if args.sqlstats:
            stats = dict(stats)
        try:
            if not args.no_drop:
                with timing("drop-tables"):
                    drop_tables(engine, meta, schema_name, table_names)
        finally:
            pprint(timing.timing, sort_dicts=False)
            if args.sqlstats:
                _print_query_stats(stats)


def timer():
    timing = {}

    @contextmanager
    def track_time(name):
        s = time.time()
        yield
        timing[name] = time.time() - s

    track_time.timing = timing
    return track_time


if __name__ == "__main__":
    parser = ArgumentParser(formatter_class=ArgumentDefaultsHelpFormatter)
    parser.add_argument(
        "--db", help="Database url", default="sqlite:///many-table.db"
    )
    parser.add_argument(
        "--schema-name",
        help="optional schema name",
        type=str,
        default=None,
    )
    parser.add_argument(
        "--table-number",
        help="Number of table to generate.",
        type=int,
        default=250,
    )
    parser.add_argument(
        "--min-cols",
        help="Min number of column per table.",
        type=int,
        default=15,
    )
    parser.add_argument(
        "--max-cols",
        help="Max number of column per table.",
        type=int,
        default=250,
    )
    parser.add_argument(
        "--no-create", help="Do not run create tables", action="store_true"
    )
    parser.add_argument(
        "--no-drop", help="Do not run drop tables", action="store_true"
    )
    parser.add_argument("--reflect", help="Run reflect", action="store_true")
    parser.add_argument(
        "--test",
        help="Run these tests. 'all' runs all tests",
        nargs="+",
        choices=tuple(tests) + ("all", "none"),
        default=["all"],
    )
    parser.add_argument(
        "--sqlstats",
        help="count and time individual queries",
        action="store_true",
    )
    parser.add_argument(
        "--multi-only", help="Only run multi table tests", action="store_true"
    )
    parser.add_argument(
        "--single-only",
        help="Only run single table tests",
        action="store_true",
    )
    parser.add_argument(
        "--echo", action="store_true", help="Enable echo on the engine"
    )
    parser.add_argument(
        "--ignore-diff",
        action="store_true",
        help="Ignores differences in the single/multi reflections",
    )
    parser.add_argument(
        "--single-inspect-conn",
        action="store_true",
        help="Uses inspect on a connection instead of on the engine when "
        "using single reflections. Mainly for sqlite.",
    )
    parser.add_argument("--pool-class", help="The pool class to use")

    args = parser.parse_args()
    min_cols = args.min_cols
    max_cols = args.max_cols
    USE_CONNECTION = args.single_inspect_conn
    assert min_cols <= max_cols and min_cols >= 1
    assert not (args.multi_only and args.single_only)
    main(
        args.db, args.schema_name, args.table_number, min_cols, max_cols, args
    )
