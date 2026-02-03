# NOTE: typing implementation is full of heuristic so unit test it to avoid
# unexpected breakages.

import operator
import typing
from typing import cast

import typing_extensions

from sqlalchemy import Column
from sqlalchemy import util
from sqlalchemy.testing import eq_
from sqlalchemy.testing import fixtures
from sqlalchemy.testing import is_
from sqlalchemy.testing import requires
from sqlalchemy.util import py310
from sqlalchemy.util import py312
from sqlalchemy.util import py314
from sqlalchemy.util import py38
from sqlalchemy.util import typing as sa_typing

TV = typing.TypeVar("TV")


def union_types():
    res = [typing.Union[int, str]]
    if py310:
        res.append(int | str)
    return res


def null_union_types():
    res = [
        typing.Optional[typing.Union[int, str]],
        typing.Union[int, str, None],
        typing.Union[int, str, "None"],
    ]
    if py310:
        res.append(int | str | None)
        res.append(typing.Optional[int | str])
        res.append(typing.Union[int, str] | None)
        res.append(typing.Optional[int] | str)
    return res


def generic_unions():
    # remove new-style unions `int | str` that are not generic
    res = union_types() + null_union_types()
    if py310 and not py314:
        new_ut = type(int | str)
        res = [t for t in res if not isinstance(t, new_ut)]
    return res


def make_fw_ref(anno: str) -> typing.ForwardRef:
    return typing.Union[anno]


TypeAliasType = getattr(
    typing, "TypeAliasType", typing_extensions.TypeAliasType
)

TA_int = TypeAliasType("TA_int", int)
TAext_int = typing_extensions.TypeAliasType("TAext_int", int)
TA_union = TypeAliasType("TA_union", typing.Union[int, str])
TAext_union = typing_extensions.TypeAliasType(
    "TAext_union", typing.Union[int, str]
)
TA_null_union = TypeAliasType("TA_null_union", typing.Union[int, str, None])
TAext_null_union = typing_extensions.TypeAliasType(
    "TAext_null_union", typing.Union[int, str, None]
)
TA_null_union2 = TypeAliasType(
    "TA_null_union2", typing.Union[int, str, "None"]
)
TAext_null_union2 = typing_extensions.TypeAliasType(
    "TAext_null_union2", typing.Union[int, str, "None"]
)
TA_null_union3 = TypeAliasType(
    "TA_null_union3", typing.Union[int, "typing.Union[None, bool]"]
)
TAext_null_union3 = typing_extensions.TypeAliasType(
    "TAext_null_union3", typing.Union[int, "typing.Union[None, bool]"]
)
TA_null_union4 = TypeAliasType(
    "TA_null_union4", typing.Union[int, "TA_null_union2"]
)
TAext_null_union4 = typing_extensions.TypeAliasType(
    "TAext_null_union4", typing.Union[int, "TAext_null_union2"]
)
TA_union_ta = TypeAliasType("TA_union_ta", typing.Union[TA_int, str])
TAext_union_ta = typing_extensions.TypeAliasType(
    "TAext_union_ta", typing.Union[TAext_int, str]
)
TA_null_union_ta = TypeAliasType(
    "TA_null_union_ta", typing.Union[TA_null_union, float]
)
TAext_null_union_ta = typing_extensions.TypeAliasType(
    "TAext_null_union_ta", typing.Union[TAext_null_union, float]
)
TA_list = TypeAliasType(
    "TA_list", typing.Union[int, str, typing.List["TA_list"]]
)
TAext_list = typing_extensions.TypeAliasType(
    "TAext_list", typing.Union[int, str, typing.List["TAext_list"]]
)
# these below not valid. Verify that it does not cause exceptions in any case
TA_recursive = TypeAliasType("TA_recursive", typing.Union["TA_recursive", str])
TAext_recursive = typing_extensions.TypeAliasType(
    "TAext_recursive", typing.Union["TAext_recursive", str]
)
TA_null_recursive = TypeAliasType(
    "TA_null_recursive", typing.Union[TA_recursive, None]
)
TAext_null_recursive = typing_extensions.TypeAliasType(
    "TAext_null_recursive", typing.Union[TAext_recursive, None]
)
TA_recursive_a = TypeAliasType(
    "TA_recursive_a", typing.Union["TA_recursive_b", int]
)
TAext_recursive_a = typing_extensions.TypeAliasType(
    "TAext_recursive_a", typing.Union["TAext_recursive_b", int]
)
TA_recursive_b = TypeAliasType(
    "TA_recursive_b", typing.Union["TA_recursive_a", str]
)
TAext_recursive_b = typing_extensions.TypeAliasType(
    "TAext_recursive_b", typing.Union["TAext_recursive_a", str]
)
TA_generic = TypeAliasType("TA_generic", typing.List[TV], type_params=(TV,))
TAext_generic = typing_extensions.TypeAliasType(
    "TAext_generic", typing.List[TV], type_params=(TV,)
)
TA_generic_typed = TA_generic[int]
TAext_generic_typed = TAext_generic[int]
TA_generic_null = TypeAliasType(
    "TA_generic_null", typing.Union[typing.List[TV], None], type_params=(TV,)
)
TAext_generic_null = typing_extensions.TypeAliasType(
    "TAext_generic_null",
    typing.Union[typing.List[TV], None],
    type_params=(TV,),
)
TA_generic_null_typed = TA_generic_null[str]
TAext_generic_null_typed = TAext_generic_null[str]


def type_aliases():
    return [
        TA_int,
        TAext_int,
        TA_union,
        TAext_union,
        TA_null_union,
        TAext_null_union,
        TA_null_union2,
        TAext_null_union2,
        TA_null_union3,
        TAext_null_union3,
        TA_null_union4,
        TAext_null_union4,
        TA_union_ta,
        TAext_union_ta,
        TA_null_union_ta,
        TAext_null_union_ta,
        TA_list,
        TAext_list,
        TA_recursive,
        TAext_recursive,
        TA_null_recursive,
        TAext_null_recursive,
        TA_recursive_a,
        TAext_recursive_a,
        TA_recursive_b,
        TAext_recursive_b,
        TA_generic,
        TAext_generic,
        TA_generic_typed,
        TAext_generic_typed,
        TA_generic_null,
        TAext_generic_null,
        TA_generic_null_typed,
        TAext_generic_null_typed,
    ]


NT_str = typing.NewType("NT_str", str)
NT_null = typing.NewType("NT_null", None)
# this below is not valid. Verify that it does not cause exceptions in any case
NT_union = typing.NewType("NT_union", typing.Union[str, int])


def new_types():
    return [NT_str, NT_null, NT_union]


A_str = typing_extensions.Annotated[str, "meta"]
A_null_str = typing_extensions.Annotated[
    typing.Union[str, None], "other_meta", "null"
]
A_union = typing_extensions.Annotated[typing.Union[str, int], "other_meta"]
A_null_union = typing_extensions.Annotated[
    typing.Union[str, int, None], "other_meta", "null"
]


def compare_type_by_string(a, b):
    """python 3.14 has made ForwardRefs not really comparable or reliably
    hashable.

    As we need to compare types here, including structures like
    `Union["str", "int"]`, without having to dive into cpython's source code
    each time a new release comes out, compare based on stringification,
    which still presents changing rules but at least are easy to diagnose
    and correct for different python versions.

    See discussion at https://github.com/python/cpython/issues/129463
    for background

    """

    if isinstance(a, (set, list)):
        a = sorted(a, key=lambda x: str(x))
    if isinstance(b, (set, list)):
        b = sorted(b, key=lambda x: str(x))

    eq_(str(a), str(b))


def annotated_l():
    return [A_str, A_null_str, A_union, A_null_union]


def all_types():
    return (
        union_types()
        + null_union_types()
        + type_aliases()
        + new_types()
        + annotated_l()
    )


def exec_code(code: str, *vars: str) -> typing.Any:
    assert vars
    scope = {}
    exec(code, None, scope)
    if len(vars) == 1:
        return scope[vars[0]]
    return [scope[name] for name in vars]


class TestGenerics(fixtures.TestBase):
    def test_traversible_is_generic(self):
        """test #6759"""
        col = Column[int]

        # looked in the source for typing._GenericAlias.
        # col.__origin__ is Column, but it's not public API.
        # __reduce__ could change too but seems good enough for now
        eq_(cast(object, col).__reduce__(), (operator.getitem, (Column, int)))


class TestTestingThings(fixtures.TestBase):
    def test_unions_are_the_same(self):
        # the point of this test is to reduce the cases to test since
        # some symbols are the same in typing and typing_extensions.
        # If a test starts failing then additional cases should be added,
        # similar to what it's done for TypeAliasType

        # no need to test typing_extensions.Union, typing_extensions.Optional
        is_(typing.Union, typing_extensions.Union)
        is_(typing.Optional, typing_extensions.Optional)

    @requires.python312
    def test_make_type_alias_type(self):
        # verify that TypeAliasType('foo', int) it the same as 'type foo = int'
        x_type = exec_code("type x = int", "x")
        x = typing.TypeAliasType("x", int)

        eq_(type(x_type), type(x))
        eq_(x_type.__name__, x.__name__)
        eq_(x_type.__value__, x.__value__)

    def test_make_fw_ref(self):
        compare_type_by_string(make_fw_ref("str"), typing.ForwardRef("str"))
        compare_type_by_string(
            make_fw_ref("str|int"), typing.ForwardRef("str|int")
        )
        compare_type_by_string(
            make_fw_ref("Optional[Union[str, int]]"),
            typing.ForwardRef("Optional[Union[str, int]]"),
        )


class TestTyping(fixtures.TestBase):
    def test_is_pep593(self):
        eq_(sa_typing.is_pep593(str), False)
        eq_(sa_typing.is_pep593(None), False)
        eq_(sa_typing.is_pep593(typing_extensions.Annotated[int, "a"]), True)
        if py310:
            eq_(sa_typing.is_pep593(typing.Annotated[int, "a"]), True)

        for t in annotated_l():
            eq_(sa_typing.is_pep593(t), True)
        for t in (
            union_types() + null_union_types() + type_aliases() + new_types()
        ):
            eq_(sa_typing.is_pep593(t), False)

    def test_is_literal(self):
        if py38:
            eq_(sa_typing.is_literal(typing.Literal["a"]), True)
        eq_(sa_typing.is_literal(typing_extensions.Literal["a"]), True)
        eq_(sa_typing.is_literal(None), False)
        for t in all_types():
            eq_(sa_typing.is_literal(t), False)

    def test_is_newtype(self):
        eq_(sa_typing.is_newtype(str), False)

        for t in new_types():
            eq_(sa_typing.is_newtype(t), True)
        for t in (
            union_types() + null_union_types() + type_aliases() + annotated_l()
        ):
            eq_(sa_typing.is_newtype(t), False)

    def test_is_generic(self):
        class W(typing.Generic[TV]):
            pass

        eq_(sa_typing.is_generic(typing.List[int]), True)
        eq_(sa_typing.is_generic(W), False)
        eq_(sa_typing.is_generic(W[str]), True)

        if py312:
            t = exec_code("class W[T]: pass", "W")
            eq_(sa_typing.is_generic(t), False)
            eq_(sa_typing.is_generic(t[int]), True)

        generics = [
            TA_generic_typed,
            TAext_generic_typed,
            TA_generic_null_typed,
            TAext_generic_null_typed,
            *annotated_l(),
            *generic_unions(),
        ]

        for t in all_types():
            if py314:
                exp = any(t == k for k in generics)
            else:
                # use is since union compare equal between new/old style
                exp = any(t is k for k in generics)
            eq_(sa_typing.is_generic(t), exp, t)

    def test_is_pep695(self):
        eq_(sa_typing.is_pep695(str), False)
        for t in (
            union_types() + null_union_types() + new_types() + annotated_l()
        ):
            eq_(sa_typing.is_pep695(t), False)
        for t in type_aliases():
            eq_(sa_typing.is_pep695(t), True)

    @requires.python38
    def test_pep695_value(self):
        eq_(sa_typing.pep695_values(int), {int})
        eq_(
            sa_typing.pep695_values(typing.Union[int, str]),
            {typing.Union[int, str]},
        )

        for t in (
            union_types() + null_union_types() + new_types() + annotated_l()
        ):
            eq_(sa_typing.pep695_values(t), {t})

        eq_(
            sa_typing.pep695_values(typing.Union[int, TA_int]),
            {typing.Union[int, TA_int]},
        )
        eq_(
            sa_typing.pep695_values(typing.Union[int, TAext_int]),
            {typing.Union[int, TAext_int]},
        )

        eq_(sa_typing.pep695_values(TA_int), {int})
        eq_(sa_typing.pep695_values(TAext_int), {int})
        eq_(sa_typing.pep695_values(TA_union), {int, str})
        eq_(sa_typing.pep695_values(TAext_union), {int, str})
        eq_(sa_typing.pep695_values(TA_null_union), {int, str, None})
        eq_(sa_typing.pep695_values(TAext_null_union), {int, str, None})
        eq_(sa_typing.pep695_values(TA_null_union2), {int, str, None})
        eq_(sa_typing.pep695_values(TAext_null_union2), {int, str, None})

        compare_type_by_string(
            sa_typing.pep695_values(TA_null_union3),
            [int, typing.ForwardRef("typing.Union[None, bool]")],
        )

        compare_type_by_string(
            sa_typing.pep695_values(TAext_null_union3),
            {int, typing.ForwardRef("typing.Union[None, bool]")},
        )

        compare_type_by_string(
            sa_typing.pep695_values(TA_null_union4),
            [int, typing.ForwardRef("TA_null_union2")],
        )
        compare_type_by_string(
            sa_typing.pep695_values(TAext_null_union4),
            {int, typing.ForwardRef("TAext_null_union2")},
        )

        eq_(sa_typing.pep695_values(TA_union_ta), {int, str})
        eq_(sa_typing.pep695_values(TAext_union_ta), {int, str})
        eq_(sa_typing.pep695_values(TA_null_union_ta), {int, str, None, float})

        compare_type_by_string(
            sa_typing.pep695_values(TAext_null_union_ta),
            {int, str, None, float},
        )

        compare_type_by_string(
            sa_typing.pep695_values(TA_list),
            [int, str, typing.List[typing.ForwardRef("TA_list")]],
        )

        compare_type_by_string(
            sa_typing.pep695_values(TAext_list),
            {int, str, typing.List[typing.ForwardRef("TAext_list")]},
        )

        compare_type_by_string(
            sa_typing.pep695_values(TA_recursive),
            [str, typing.ForwardRef("TA_recursive")],
        )
        compare_type_by_string(
            sa_typing.pep695_values(TAext_recursive),
            {typing.ForwardRef("TAext_recursive"), str},
        )
        compare_type_by_string(
            sa_typing.pep695_values(TA_null_recursive),
            [str, typing.ForwardRef("TA_recursive"), None],
        )
        compare_type_by_string(
            sa_typing.pep695_values(TAext_null_recursive),
            {typing.ForwardRef("TAext_recursive"), str, None},
        )
        compare_type_by_string(
            sa_typing.pep695_values(TA_recursive_a),
            [int, typing.ForwardRef("TA_recursive_b")],
        )
        compare_type_by_string(
            sa_typing.pep695_values(TAext_recursive_a),
            {typing.ForwardRef("TAext_recursive_b"), int},
        )
        compare_type_by_string(
            sa_typing.pep695_values(TA_recursive_b),
            [str, typing.ForwardRef("TA_recursive_a")],
        )
        compare_type_by_string(
            sa_typing.pep695_values(TAext_recursive_b),
            {typing.ForwardRef("TAext_recursive_a"), str},
        )

    @requires.up_to_date_typealias_type
    def test_pep695_value_generics(self):
        # generics

        eq_(sa_typing.pep695_values(TA_generic), {typing.List[TV]})
        eq_(sa_typing.pep695_values(TAext_generic), {typing.List[TV]})
        eq_(sa_typing.pep695_values(TA_generic_typed), {typing.List[TV]})
        eq_(sa_typing.pep695_values(TAext_generic_typed), {typing.List[TV]})
        eq_(sa_typing.pep695_values(TA_generic_null), {None, typing.List[TV]})
        eq_(
            sa_typing.pep695_values(TAext_generic_null),
            {None, typing.List[TV]},
        )
        eq_(
            sa_typing.pep695_values(TA_generic_null_typed),
            {None, typing.List[TV]},
        )
        eq_(
            sa_typing.pep695_values(TAext_generic_null_typed),
            {None, typing.List[TV]},
        )

    def test_is_fwd_ref(self):
        eq_(sa_typing.is_fwd_ref(int), False)
        eq_(sa_typing.is_fwd_ref(make_fw_ref("str")), True)
        eq_(sa_typing.is_fwd_ref(typing.Union[str, int]), False)
        eq_(sa_typing.is_fwd_ref(typing.Union["str", int]), False)
        eq_(sa_typing.is_fwd_ref(typing.Union["str", int], True), True)

        for t in all_types():
            eq_(sa_typing.is_fwd_ref(t), False)

    def test_de_optionalize_union_types(self):
        fn = sa_typing.de_optionalize_union_types

        eq_(
            fn(typing.Optional[typing.Union[int, str]]), typing.Union[int, str]
        )
        eq_(fn(typing.Union[int, str, None]), typing.Union[int, str])

        eq_(fn(typing.Union[int, str, "None"]), typing.Union[int, str])

        eq_(fn(make_fw_ref("None")), typing_extensions.Never)
        eq_(fn(make_fw_ref("typing.Union[None]")), typing_extensions.Never)
        eq_(fn(make_fw_ref("Union[None, str]")), typing.ForwardRef("str"))

        compare_type_by_string(
            fn(make_fw_ref("Union[None, str, int]")),
            typing.Union["str", "int"],
        )

        compare_type_by_string(
            fn(make_fw_ref("Optional[int]")), typing.ForwardRef("int")
        )

        compare_type_by_string(
            fn(make_fw_ref("typing.Optional[Union[int | str]]")),
            typing.ForwardRef("Union[int | str]"),
        )

        for t in null_union_types():
            res = fn(t)
            eq_(sa_typing.is_union(res), True)
            eq_(type(None) not in res.__args__, True)

        for t in union_types() + type_aliases() + new_types() + annotated_l():
            eq_(fn(t), t)

        compare_type_by_string(
            fn(make_fw_ref("Union[typing.Dict[str, int], int, None]")),
            typing.Union[
                "typing.Dict[str, int]",
                "int",
            ],
        )

    def test_make_union_type(self):
        eq_(sa_typing.make_union_type(int), int)
        eq_(sa_typing.make_union_type(None), type(None))
        eq_(sa_typing.make_union_type(int, str), typing.Union[int, str])
        eq_(
            sa_typing.make_union_type(int, typing.Optional[str]),
            typing.Union[int, str, None],
        )
        eq_(
            sa_typing.make_union_type(int, typing.Union[str, bool]),
            typing.Union[int, str, bool],
        )
        eq_(
            sa_typing.make_union_type(bool, TA_int, NT_str),
            typing.Union[bool, TA_int, NT_str],
        )
        eq_(
            sa_typing.make_union_type(bool, TAext_int, NT_str),
            typing.Union[bool, TAext_int, NT_str],
        )

    @requires.up_to_date_typealias_type
    @requires.python38
    def test_includes_none_generics(self):
        # TODO: these are false negatives
        false_negatives = {
            TA_null_union4,  # does not evaluate FW ref
            TAext_null_union4,  # does not evaluate FW ref
        }
        for t in type_aliases() + new_types():
            if t in false_negatives:
                exp = False
            else:
                exp = "null" in t.__name__
            eq_(sa_typing.includes_none(t), exp, str(t))

    @requires.python38
    def test_includes_none(self):
        eq_(sa_typing.includes_none(None), True)
        eq_(sa_typing.includes_none(type(None)), True)
        eq_(sa_typing.includes_none(typing.ForwardRef("None")), True)
        eq_(sa_typing.includes_none(int), False)
        for t in union_types():
            eq_(sa_typing.includes_none(t), False)

        for t in null_union_types():
            eq_(sa_typing.includes_none(t), True, str(t))

        for t in annotated_l():
            eq_(
                sa_typing.includes_none(t),
                "null" in sa_typing.get_args(t),
                str(t),
            )
        # nested things
        eq_(sa_typing.includes_none(typing.Union[int, "None"]), True)
        eq_(sa_typing.includes_none(typing.Union[bool, TA_null_union]), True)
        eq_(
            sa_typing.includes_none(typing.Union[bool, TAext_null_union]), True
        )
        eq_(sa_typing.includes_none(typing.Union[bool, NT_null]), True)
        # nested fw
        eq_(
            sa_typing.includes_none(
                typing.Union[int, "typing.Union[str, None]"]
            ),
            True,
        )
        eq_(
            sa_typing.includes_none(
                typing.Union[int, "typing.Union[int, str]"]
            ),
            False,
        )

        # there are not supported. should return True
        eq_(
            sa_typing.includes_none(typing.Union[bool, "TA_null_union"]), False
        )
        eq_(
            sa_typing.includes_none(typing.Union[bool, "TAext_null_union"]),
            False,
        )
        eq_(sa_typing.includes_none(typing.Union[bool, "NT_null"]), False)

    def test_is_union(self):
        eq_(sa_typing.is_union(str), False)
        for t in union_types() + null_union_types():
            eq_(sa_typing.is_union(t), True)
        for t in type_aliases() + new_types() + annotated_l():
            eq_(sa_typing.is_union(t), False)

    def test_TypingInstances(self):
        is_(sa_typing._type_tuples, sa_typing._type_instances)
        is_(
            isinstance(sa_typing._type_instances, sa_typing._TypingInstances),
            True,
        )

        # cached
        is_(
            sa_typing._type_instances.Literal,
            sa_typing._type_instances.Literal,
        )

        for k in ["Literal", "Annotated", "TypeAliasType"]:
            types = set()
            ti = getattr(sa_typing._type_instances, k)
            for lib in [typing, typing_extensions]:
                lt = getattr(lib, k, None)
                if lt is not None:
                    types.add(lt)
                    is_(lt in ti, True)
            eq_(len(ti), len(types), k)

    @requires.pep649
    def test_pep649_getfullargspec(self):
        """test for #13104"""

        def foo(x: Frobnizzle):  # type: ignore  # noqa: F821
            pass

        anno = util.get_annotations(foo)
        eq_(
            util.inspect_getfullargspec(foo),
            util.compat.FullArgSpec(
                args=["x"],
                varargs=None,
                varkw=None,
                defaults=None,
                kwonlyargs=[],
                kwonlydefaults=None,
                annotations=anno,
            ),
        )
