#!/usr/bin/env bash
set -uo pipefail

cd /app

OUTPUT_PATH=""
if [ "${1:-}" = "--output_path" ]; then
  OUTPUT_PATH="$2"
  shift 2
fi

MODE="${1:-new}"

case "$MODE" in
  base)
    python3 -m pytest \
      test/dialect/postgresql/ \
      test/sql/test_ddlemit.py \
      --ignore=test/dialect/postgresql/test_unnamed_index_57ab14.py \
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
      "test/dialect/postgresql/test_compiler.py::CompileTest::test_create_index_named_without_unnamed_is_unaffected" \
      "test/dialect/postgresql/test_compiler.py::CompileTest::test_create_index_none_name_without_unnamed_still_autonames" \
      -v \
      ${OUTPUT_PATH:+--junitxml="$OUTPUT_PATH"}
    ;;
  *)
    echo "unknown mode: $MODE (expected base or new)" >&2
    exit 2
    ;;
esac

exit $?
