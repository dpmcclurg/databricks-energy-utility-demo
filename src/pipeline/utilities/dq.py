"""Builds pipeline expectations from the FP&A-owned rules table (cgu_fpa.dq_rules).

The rules are read once when the pipeline update starts, so an FP&A rule change takes effect on
the next update without a code change or redeploy.
"""
from pyspark.sql import SparkSession


def load_rules(spark: SparkSession, catalog: str, target: str) -> dict:
    """Return {action: {rule_name: constraint_sql}} for one target dataset."""
    rows = (spark.read.table(f"`{catalog}`.cgu_fpa.dq_rules")
            .where(f"target = '{target}'")
            .select("rule_name", "constraint_sql", "action")
            .collect())
    rules = {"warn": {}, "drop": {}, "fail": {}}
    for r in rows:
        rules[r.action][r.rule_name] = r.constraint_sql
    return rules


def skip_deletes(rules: dict) -> dict:
    """CDC deletes carry no row values, so rules only apply to inserts and updates."""
    return {name: f"op = 'd' OR ({cond})" for name, cond in rules.items()}


def expectations(rules: dict):
    """Decorator that attaches warn, drop, and fail expectations, skipping any empty group."""
    from pyspark import pipelines as dp

    def wrap(fn):
        if rules.get("fail"):
            fn = dp.expect_all_or_fail(rules["fail"])(fn)
        if rules.get("drop"):
            fn = dp.expect_all_or_drop(rules["drop"])(fn)
        if rules.get("warn"):
            fn = dp.expect_all(rules["warn"])(fn)
        return fn
    return wrap
