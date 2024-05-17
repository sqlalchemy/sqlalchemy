from collections import defaultdict

from .base import Case

if True:
    from . import cache_key  # noqa: F401
    from . import collections_  # noqa: F401
    from . import misc  # noqa: F401
    from . import row  # noqa: F401


def tabulate(
    result_by_impl: dict[str, dict[str, float]],
    result_by_method: dict[str, dict[str, float]],
):
    if not result_by_method:
        return
    dim = 11

    width = max(20, *(len(m) + 1 for m in result_by_method))

    string_cell = "{:<%s}" % dim
    header = "{:<%s}|" % width + f" {string_cell} |" * len(result_by_impl)
    num_format = "{:<%s.9f}" % dim
    csv_row = "{:<%s}|" % width + " {} |" * len(result_by_impl)
    names = list(result_by_impl)
    print(header.format("", *names))

    for meth in result_by_method:
        data = result_by_method[meth]
        strings = [
            (
                num_format.format(data[name])[:dim]
                if name in data
                else string_cell.format("—")
            )
            for name in names
        ]
        print(csv_row.format(meth, *strings))


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

    args = parser.parse_args()

    if "all" in args.case:
        to_run = cases
    else:
        to_run = [c for c in cases if c.__name__ in args.case]

    for case in to_run:
        print("Running case", case.__name__)
        result_by_impl = case.run_case(args.factor, args.filter)

        result_by_method = defaultdict(dict)
        for name in result_by_impl:
            for meth in result_by_impl[name]:
                result_by_method[meth][name] = result_by_impl[name][meth]

        tabulate(result_by_impl, result_by_method)

        if args.csv:
            import csv

            file_name = f"{case.__name__}.csv"
            with open(file_name, "w", newline="") as f:
                w = csv.DictWriter(f, ["", *result_by_impl])
                w.writeheader()
                for n in result_by_method:
                    w.writerow({"": n, **result_by_method[n]})
            print("Wrote file", file_name)
