from collections import defaultdict
from datetime import datetime
import subprocess

import sqlalchemy as sa
from .base import Case

if True:
    from . import cache_key  # noqa: F401
    from . import collections_  # noqa: F401
    from . import misc  # noqa: F401
    from . import result  # noqa: F401
    from . import row  # noqa: F401


def tabulate(
    impl_names: list[str],
    result_by_method: dict[str, dict[str, float]],
):
    if not result_by_method:
        return
    dim = max(len(n) for n in impl_names)
    dim = min(dim, 20)

    width = max(20, *(len(m) + 1 for m in result_by_method))

    string_cell = "{:<%s}" % dim
    header = "{:<%s}|" % width + f" {string_cell} |" * len(impl_names)
    num_format = "{:<%s.9f}" % dim
    csv_row = "{:<%s}|" % width + " {} |" * len(impl_names)
    print(header.format("", *impl_names))

    for meth in result_by_method:
        data = result_by_method[meth]
        strings = [
            (
                num_format.format(data[name])[:dim]
                if name in data
                else string_cell.format("—")
            )
            for name in impl_names
        ]
        print(csv_row.format(meth, *strings))


def find_git_sha():
    try:
        git_res = subprocess.run(
            ["git", "rev-parse", "--short", "HEAD"], stdout=subprocess.PIPE
        )
        return git_res.stdout.decode("utf-8").strip()
    except Exception:
        return None


def main():
    import argparse

    cases = Case._CASES

    parser = argparse.ArgumentParser(
        description="Compare implementation between them"
    )
    parser.add_argument(
        "case",
        help="Case to run",
        nargs="+",
        choices=["all"] + sorted(c.__name__ for c in cases),
    )
    parser.add_argument("--filter", help="filter the test for this regexp")
    parser.add_argument(
        "--factor", help="scale number passed to timeit", type=float, default=1
    )
    parser.add_argument("--csv", help="save to csv", action="store_true")
    save_group = parser.add_argument_group("Save result for later compare")
    save_group.add_argument(
        "--save-db",
        help="Name of the sqlite db file to use",
        const="perf.db",
        nargs="?",
    )
    save_group.add_argument(
        "--save-name",
        help="A name given to the current save. "
        "Can be used later to compare against this run.",
    )

    compare_group = parser.add_argument_group("Compare against stored data")
    compare_group.add_argument(
        "--compare-db",
        help="Name of the sqlite db file to read for the compare data",
        const="perf.db",
        nargs="?",
    )
    compare_group.add_argument(
        "--compare-filter",
        help="Filter the compare data using this string. Can include "
        "git-short-sha, save-name previously used or date. By default the "
        "latest values are used",
    )

    args = parser.parse_args()

    to_run: list[type[Case]]
    if "all" in args.case:
        to_run = cases
    else:
        to_run = [c for c in cases if c.__name__ in args.case]

    if args.save_db:
        save_engine = sa.create_engine(
            f"sqlite:///{args.save_db}", poolclass=sa.NullPool
        )
        PerfTable.metadata.create_all(save_engine)
        sha = find_git_sha()

    if args.compare_db:
        compare_engine = sa.create_engine(
            f"sqlite:///{args.compare_db}", poolclass=sa.NullPool
        )
        stmt = (
            sa.select(PerfTable)
            .where(PerfTable.c.factor == args.factor)
            .order_by(PerfTable.c.created.desc())
        )
        if args.compare_filter:
            cf = args.compare_filter
            stmt = stmt.where(
                sa.or_(
                    PerfTable.c.created.cast(sa.Text).icontains(cf),
                    PerfTable.c.git_short_sha.icontains(cf),
                    PerfTable.c.save_name.icontains(cf),
                ),
            )

    for case in to_run:
        print("Running case", case.__name__)
        if args.compare_db:
            with compare_engine.connect() as conn:
                case_stmt = stmt.where(PerfTable.c.case == case.__name__)
                compare_by_meth = defaultdict(dict)
                for prow in conn.execute(case_stmt):
                    if prow.impl in compare_by_meth[prow.method]:
                        continue
                    compare_by_meth[prow.method][prow.impl] = prow.value
        else:
            compare_by_meth = {}

        result_by_impl, impl_names = case.run_case(args.factor, args.filter)

        result_by_method = defaultdict(dict)
        all_impls = dict.fromkeys(result_by_impl)
        for impl in result_by_impl:
            for meth in result_by_impl[impl]:
                meth_dict = result_by_method[meth]
                meth_dict[impl] = result_by_impl[impl][meth]
                if meth in compare_by_meth and impl in compare_by_meth[meth]:
                    cmp_impl = f"compare {impl}"
                    over = f"{impl} / compare"
                    all_impls[cmp_impl] = None
                    all_impls[over] = None
                    meth_dict[cmp_impl] = compare_by_meth[meth][impl]
                    meth_dict[over] = meth_dict[impl] / meth_dict[cmp_impl]

        tabulate(list(all_impls), result_by_method)

        if args.csv:
            import csv

            file_name = f"{case.__name__}.csv"
            with open(file_name, "w", newline="") as f:
                w = csv.DictWriter(f, ["", *result_by_impl])
                w.writeheader()
                for n in result_by_method:
                    w.writerow({"": n, **result_by_method[n]})
            print("Wrote file", file_name)

        if args.save_db:
            data = [
                {
                    "case": case.__name__,
                    "impl": impl,
                    "method": meth,
                    "value": result_by_impl[impl][meth],
                    "factor": args.factor,
                    "save_name": args.save_name,
                    "git_short_sha": sha,
                    "created": Now,
                }
                for impl in impl_names
                for meth in result_by_impl[impl]
            ]
            with save_engine.begin() as conn:
                conn.execute(PerfTable.insert(), data)


PerfTable = sa.Table(
    "perf_table",
    sa.MetaData(),
    sa.Column("case", sa.Text, nullable=False),
    sa.Column("impl", sa.Text, nullable=False),
    sa.Column("method", sa.Text, nullable=False),
    sa.Column("value", sa.Float),
    sa.Column("factor", sa.Float),
    sa.Column("save_name", sa.Text),
    sa.Column("git_short_sha", sa.Text),
    sa.Column("created", sa.DateTime, nullable=False),
)
Now = datetime.now()
