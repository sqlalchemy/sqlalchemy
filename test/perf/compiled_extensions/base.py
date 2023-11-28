from collections import defaultdict
import math
import re
from timeit import timeit


def test_case(fn=None, *, number=None):
    def wrap(fn):
        fn.__test_case__ = True
        if number is not None:
            fn.__number__ = number
        return fn

    if fn is None:
        return wrap
    else:
        return wrap(fn)


class Case:
    """Base test case. Mark test cases with ``test_case``"""

    IMPLEMENTATIONS = {}
    "Keys are the impl name, values are callable to load it"
    NUMBER = 1_000_000

    _CASES = []

    def __init__(self, impl):
        self.impl = impl
        self.init_objects()

    def __init_subclass__(cls):
        if not cls.__name__.startswith("_"):
            Case._CASES.append(cls)

    def init_objects(self):
        pass

    @classmethod
    def init_class(cls):
        pass

    @classmethod
    def _load(cls, fn):
        try:
            return fn()
        except Exception as e:
            print(f"Error loading {fn}: {e!r}")

    @classmethod
    def import_impl(cls):
        impl = []
        for name, fn in cls.IMPLEMENTATIONS.items():
            obj = cls._load(fn)
            if obj:
                impl.append((name, obj))
        return impl

    @classmethod
    def _divide_results(cls, results, num, div, name):
        "utility method to create ratios of two implementation"
        avg_str = "> mean of values"
        if div in results and num in results:
            num_dict = results[num]
            div_dict = results[div]
            assert avg_str not in num_dict and avg_str not in div_dict
            assert num_dict.keys() == div_dict.keys()
            results[name] = {m: num_dict[m] / div_dict[m] for m in div_dict}
            not_na = [v for v in results[name].values() if not math.isnan(v)]
            avg = sum(not_na) / len(not_na)
            results[name][avg_str] = avg

    @classmethod
    def update_results(cls, results):
        pass

    @classmethod
    def run_case(cls, factor, filter_):
        objects = cls.import_impl()
        cls.init_class()
        number = max(1, int(cls.NUMBER * factor))

        stack = [c for c in cls.mro() if c not in {object, Case}]
        methods = []
        while stack:
            curr = stack.pop(0)
            # dict keeps the definition order, dir is instead sorted
            methods += [
                m
                for m, fn in curr.__dict__.items()
                if hasattr(fn, "__test_case__")
            ]

        if filter_:
            methods = [m for m in methods if re.search(filter_, m)]

        results = defaultdict(dict)
        for name, impl in objects:
            print(f"Running {name:<10} ", end="", flush=True)
            impl_case = cls(impl)
            fails = []
            for m in methods:
                call = getattr(impl_case, m)
                try:
                    t_num = number
                    fn_num = getattr(call, "__number__", None)
                    if fn_num is not None:
                        t_num = max(1, int(fn_num * factor))
                    value = timeit(call, number=t_num)
                    print(".", end="", flush=True)
                except Exception as e:
                    fails.append(f"{name}::{m} error: {e}")
                    print("x", end="", flush=True)
                    value = float("nan")

                results[name][m] = value
            print(" Done")
            for f in fails:
                print("\t", f)

        cls.update_results(results)
        return results, [name for name, _ in objects]
