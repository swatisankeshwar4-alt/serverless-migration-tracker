"""Small Spark helpers shared by the notebooks."""


def ensure_column(spark, fq_table: str, column: str, ddl_type: str) -> None:
    """Add a column to an existing Delta table when an earlier step created it without it."""
    if column not in spark.table(fq_table).columns:
        spark.sql(f"ALTER TABLE {fq_table} ADD COLUMNS ({column} {ddl_type})")
