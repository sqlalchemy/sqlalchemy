"""Consistency checks for the packaging metadata in pyproject.toml.

These catch mistakes that setuptools accepts silently but which break
other installers when building from the source distribution.

"""

import os
import re
import tomllib

from sqlalchemy.testing import eq_
from sqlalchemy.testing import fixtures


def _normalize_extra(name):
    # PEP 685 normalization, same as PEP 503 for project names
    return re.sub(r"[-_.]+", "-", name).lower()


def _self_referenced_extras(requirements):
    for req in requirements:
        if isinstance(req, str):
            m = re.match(r"\s*sqlalchemy\s*\[([^\]]*)\]", req, re.I)
            if m:
                yield from (e.strip() for e in m.group(1).split(","))


class PyprojectTest(fixtures.TestBase):
    @classmethod
    def setup_test_class(cls):
        here = os.path.dirname(__file__)
        path = os.path.normpath(
            os.path.join(here, "..", "..", "pyproject.toml")
        )
        with open(path, "rb") as file_:
            cls.pyproject = tomllib.load(file_)

    @property
    def extras(self):
        return self.pyproject["project"]["optional-dependencies"]

    def test_extras_unique_when_normalized(self):
        """test #13604"""

        seen = {}
        for name in self.extras:
            seen.setdefault(_normalize_extra(name), []).append(name)

        eq_({k: v for k, v in seen.items() if len(v) > 1}, {})

    def test_extras_are_normalized(self):
        eq_(
            [name for name in self.extras if _normalize_extra(name) != name],
            [],
        )

    def test_self_referenced_extras_exist(self):
        requirements = [
            req for reqs in self.extras.values() for req in reqs
        ] + [
            req
            for reqs in self.pyproject.get("dependency-groups", {}).values()
            for req in reqs
        ]

        eq_(
            sorted(
                name
                for name in _self_referenced_extras(requirements)
                if name not in self.extras
            ),
            [],
        )
