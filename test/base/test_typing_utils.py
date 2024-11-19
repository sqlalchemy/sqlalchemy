# NOTE: typing implementation is full of heuristic so unit test it to avoid
# unexpected breakages.

import typing

import typing_extensions

from sqlalchemy.testing import fixtures
from sqlalchemy.testing import requires
from sqlalchemy.testing.assertions import eq_
from sqlalchemy.testing.assertions import is_
from sqlalchemy.util import py310
from sqlalchemy.util import py311
from sqlalchemy.util import py312
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


def make_fw_ref(anno: str) -> typing.ForwardRef:
    return typing.Union[anno]


TA_int = typing_extensions.TypeAliasType("TA_int", int)
TA_union = typing_extensions.TypeAliasType("TA_union", typing.Union[int, str])
TA_null_union = typing_extensions.TypeAliasType(
    "TA_null_union", typing.Union[int, str, None]
)
TA_null_union2 = typing_extensions.TypeAliasType(
    "TA_null_union2", typing.Union[int, str, "None"]
)
TA_null_union3 = typing_extensions.TypeAliasType(
    "TA_null_union3", typing.Union[int, "typing.Union[None, bool]"]
)
TA_null_union4 = typing_extensions.TypeAliasType(
    "TA_null_union4", typing.Union[int, "TA_null_union2"]
)
TA_union_ta = typing_extensions.TypeAliasType(
    "TA_union_ta", typing.Union[TA_int, str]
)
TA_null_union_ta = typing_extensions.TypeAliasType(
    "TA_null_union_ta", typing.Union[TA_null_union, float]
)
TA_list = typing_extensions.TypeAliasType(
    "TA_list", typing.Union[int, str, typing.List["TA_list"]]
)
# these below not valid. Verify that it does not cause exceptions in any case
TA_recursive = typing_extensions.TypeAliasType(
    "TA_recursive", typing.Union["TA_recursive", str]
)
TA_null_recursive = typing_extensions.TypeAliasType(
    "TA_null_recursive", typing.Union[TA_recursive, None]
)
TA_recursive_a = typing_extensions.TypeAliasType(
    "TA_recursive_a", typing.Union["TA_recursive_b", int]
)
TA_recursive_b = typing_extensions.TypeAliasType(
    "TA_recursive_b", typing.Union["TA_recursive_a", str]
)


def type_aliases():
    return [
        TA_int,
        TA_union,
        TA_null_union,
        TA_null_union2,
        TA_null_union3,
        TA_null_union4,
        TA_union_ta,
        TA_null_union_ta,
        TA_list,
        TA_recursive,
        TA_null_recursive,
        TA_recursive_a,
        TA_recursive_b,
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


class TestTestingThings(fixtures.TestBase):
    def test_unions_are_the_same(self):
        # no need to test typing_extensions.Union, typing_extensions.Optional
        is_(typing.Union, typing_extensions.Union)
        is_(typing.Optional, typing_extensions.Optional)
        if py312:
            is_(typing.TypeAliasType, typing_extensions.TypeAliasType)

    def test_make_union(self):
        v = int, str
        eq_(typing.Union[int, str], typing.Union.__getitem__(v))
        if py311:
            # need eval since it's a syntax error in python < 3.11
            eq_(typing.Union[int, str], eval("typing.Union[*(int, str)]"))
            eq_(typing.Union[int, str], eval("typing.Union[*v]"))

    @requires.python312
    def test_make_type_alias_type(self):
        # verify that TypeAliasType('foo', int) it the same as 'type foo = int'
        x_type = exec_code("type x = int", "x")
        x = typing.TypeAliasType("x", int)

        eq_(type(x_type), type(x))
        eq_(x_type.__name__, x.__name__)
        eq_(x_type.__value__, x.__value__)

    def test_make_fw_ref(self):
        eq_(make_fw_ref("str"), typing.ForwardRef("str"))
        eq_(make_fw_ref("str|int"), typing.ForwardRef("str|int"))
        eq_(
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

        for t in all_types():
            eq_(sa_typing.is_literal(t), False)

    def test_is_pep695(self):
        eq_(sa_typing.is_pep695(str), False)
        for t in (
            union_types() + null_union_types() + new_types() + annotated_l()
        ):
            eq_(sa_typing.is_pep695(t), False)
        for t in type_aliases():
            eq_(sa_typing.is_pep695(t), True)

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

        eq_(sa_typing.pep695_values(TA_int), {int})
        eq_(sa_typing.pep695_values(TA_union), {int, str})
        eq_(sa_typing.pep695_values(TA_null_union), {int, str, None})
        eq_(sa_typing.pep695_values(TA_null_union2), {int, str, None})
        eq_(
            sa_typing.pep695_values(TA_null_union3),
            {int, typing.ForwardRef("typing.Union[None, bool]")},
        )
        eq_(
            sa_typing.pep695_values(TA_null_union4),
            {int, typing.ForwardRef("TA_null_union2")},
        )
        eq_(sa_typing.pep695_values(TA_union_ta), {int, str})
        eq_(sa_typing.pep695_values(TA_null_union_ta), {int, str, None, float})
        eq_(
            sa_typing.pep695_values(TA_list),
            {int, str, typing.List[typing.ForwardRef("TA_list")]},
        )
        eq_(
            sa_typing.pep695_values(TA_recursive),
            {typing.ForwardRef("TA_recursive"), str},
        )
        eq_(
            sa_typing.pep695_values(TA_null_recursive),
            {typing.ForwardRef("TA_recursive"), str, None},
        )
        eq_(
            sa_typing.pep695_values(TA_recursive_a),
            {typing.ForwardRef("TA_recursive_b"), int},
        )
        eq_(
            sa_typing.pep695_values(TA_recursive_b),
            {typing.ForwardRef("TA_recursive_a"), str},
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
        eq_(
            fn(make_fw_ref("Union[None, str, int]")),
            typing.Union["str", "int"],
        )
        eq_(fn(make_fw_ref("Optional[int]")), typing.ForwardRef("int"))
        eq_(
            fn(make_fw_ref("typing.Optional[Union[int | str]]")),
            typing.ForwardRef("Union[int | str]"),
        )

        for t in null_union_types():
            res = fn(t)
            eq_(sa_typing.is_union(res), True)
            eq_(type(None) not in res.__args__, True)

        for t in union_types() + type_aliases() + new_types() + annotated_l():
            eq_(fn(t), t)

        eq_(
            fn(make_fw_ref("Union[typing.Dict[str, int], int, None]")),
            typing.Union["typing.Dict[str, int]", "int"],
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

    def test_includes_none(self):
        eq_(sa_typing.includes_none(None), True)
        eq_(sa_typing.includes_none(type(None)), True)
        eq_(sa_typing.includes_none(typing.ForwardRef("None")), True)
        eq_(sa_typing.includes_none(int), False)
        for t in union_types():
            eq_(sa_typing.includes_none(t), False)

        for t in null_union_types():
            eq_(sa_typing.includes_none(t), True, str(t))

        # TODO: these are false negatives
        false_negative = {
            TA_null_union4,  # does not evaluate FW ref
        }
        for t in type_aliases() + new_types():
            if t in false_negative:
                exp = False
            else:
                exp = "null" in t.__name__
            eq_(sa_typing.includes_none(t), exp, str(t))

        for t in annotated_l():
            eq_(
                sa_typing.includes_none(t),
                "null" in sa_typing.get_args(t),
                str(t),
            )
        # nested things
        eq_(sa_typing.includes_none(typing.Union[int, "None"]), True)
        eq_(sa_typing.includes_none(typing.Union[bool, TA_null_union]), True)
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
        eq_(sa_typing.includes_none(typing.Union[bool, "NT_null"]), False)

    def test_is_union(self):
        eq_(sa_typing.is_union(str), False)
        for t in union_types() + null_union_types():
            eq_(sa_typing.is_union(t), True)
        for t in type_aliases() + new_types() + annotated_l():
            eq_(sa_typing.is_union(t), False)
