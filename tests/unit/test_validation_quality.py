import pandas as pd

from src.validation import quality


def test_require_columns():
    cols = ["id", "name"]
    assert quality.require_columns(cols, ["id"]) == []
    assert quality.require_columns(cols, ["id", "age"]) == ["age"]


def test_format_reject_rate():
    assert quality.format_reject_rate(10, 100) == 0.1
    assert quality.format_reject_rate(0, 100) == 0.0
    assert quality.format_reject_rate(5, 0) == 0.0


def test_enforce_fk():
    df = pd.DataFrame({"id": [1, 2, 3]})
    ref_df = pd.DataFrame({"id": [1, 2]})
    res = quality.enforce_fk(df, "id", ref_df, "id")
    assert len(res) == 2
    assert res["id"].tolist() == [1, 2]


def test_enforce_non_null():
    df = pd.DataFrame({"id": [1, None, 3]})
    res = quality.enforce_non_null(df, ["id"])
    assert len(res) == 2
    assert res["id"].tolist() == [1.0, 3.0]


def test_validate_table():
    df = pd.DataFrame({"id": [1, None], "name": ["A", "B"]})
    config = {"required_columns": ["id", "age"], "primary_key": ["id"]}
    failures = quality.validate_table(df, config)
    assert any("missing_required_columns: ['age']" in f for f in failures)
    assert any("primary_key_nulls: 1" in f for f in failures)


def test_evaluate_expectations():
    df = pd.DataFrame({"val": [10, 20, 30], "cat": ["A", "B", "C"]})
    expectations = [
        {"type": "between", "column": "val", "min": 15, "max": 25},
        {"type": "in_set", "column": "cat", "allowed": ["A", "B"]},
        {"type": "not_null", "columns": ["val"]},
        {"type": "unique", "columns": ["cat"]},
    ]
    failures = quality.evaluate_expectations(df, expectations)
    assert len(failures) == 3  # 10 is < 15, 30 is > 25, 'C' is not in A,B
    assert any("expect_between_failed" in f for f in failures)
    assert any("expect_in_set_failed" in f for f in failures)
