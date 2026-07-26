#!/usr/bin/env bash
# Test runner for the postgresql_unnamed Index challenge.
#
# Usage:
#   ./test.sh [--output_path <junit.xml>] <base|new>
#
#   base  run the existing repository tests in the change's blast radius
#         (test/dialect/postgresql/ and test/sql/test_ddlemit.py), minus the
#         new unnamed-index tests added by this challenge -- these must pass
#         both before and after the solution is applied.
#   new   run the new unnamed-index tests; they fail before the solution and
#         pass after it.
set -uo pipefail

cd /app

OUTPUT_PATH=""
if [ "${1:-}" = "--output_path" ]; then
  OUTPUT_PATH="$2"
  shift 2
fi

MODE="${1:-new}"

# test/dialect/postgresql/test_unnamed_index.py exercises the same feature
# against a real PostgreSQL server (round-trip CREATE INDEX execution and
# checkfirst/has_index interaction). It cannot run here: this container has
# no network (--network none) and there is no PostgreSQL service available
# inside it. It stays in the repo, gated by __only_on__ = "postgresql", and
# was verified manually against a real PostgreSQL 18 instance outside this
# harness. Both modes below exclude it explicitly rather than relying on
# __only_on__ to skip it, so its exclusion doesn't depend on whatever
# backend happens to be configured by default.

case "$MODE" in
  base)
    # -k "not unnamed" deselects the 9 new unnamed-index tests in
    # test_compiler.py (they belong to "new", not "base": at this point in
    # the workflow the test patch is already applied, so they exist in the
    # file, but the solution patch may not be applied yet, and they are
    # expected to fail until it is). It also happens to deselect four
    # unrelated pre-existing tests in InsertOnConflictTest whose names
    # happen to contain "unnamed" (test_do_update_unnamed_*) -- they are
    # outside this change's blast radius.
    python3 -m pytest \
      test/dialect/postgresql/ \
      test/sql/test_ddlemit.py \
      --ignore=test/dialect/postgresql/test_unnamed_index.py \
      -k "not unnamed" \
      -v \
      ${OUTPUT_PATH:+--junitxml="$OUTPUT_PATH"}
    ;;
  new)
    python3 -m pytest \
      "test/dialect/postgresql/test_compiler.py::CompileTest::test_create_index_unnamed" \
      "test/dialect/postgresql/test_compiler.py::CompileTest::test_create_index_unnamed_unique" \
      "test/dialect/postgresql/test_compiler.py::CompileTest::test_create_index_unnamed_concurrently" \
      "test/dialect/postgresql/test_compiler.py::CompileTest::test_create_index_unnamed_noop_on_other_dialect" \
      "test/dialect/postgresql/test_compiler.py::CompileTest::test_create_index_unnamed_kwarg_defaults_false" \
      "test/dialect/postgresql/test_compiler.py::CompileTest::test_create_index_unnamed_construction_does_not_raise" \
      "test/dialect/postgresql/test_compiler.py::CompileTest::test_create_index_unnamed_with_if_not_exists_raises" \
      "test/dialect/postgresql/test_compiler.py::CompileTest::test_create_index_unnamed_with_if_not_exists_noop_on_other_dialect" \
      "test/dialect/postgresql/test_compiler.py::CompileTest::test_create_index_unnamed_with_explicit_name_raises" \
      -v \
      ${OUTPUT_PATH:+--junitxml="$OUTPUT_PATH"}
    ;;
  *)
    echo "unknown mode: $MODE (expected base or new)" >&2
    exit 2
    ;;
esac

exit $?
