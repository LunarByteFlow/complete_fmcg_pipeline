import pytest

from src.integrations.snowflake import load_staged_exports


class FakeCursor:
    def __init__(self):
        self.statements = []
        self.closed = False

    def execute(self, statement):
        self.statements.append(statement)

    def close(self):
        self.closed = True


class FakeConnection:
    def __init__(self):
        self.fake_cursor = FakeCursor()

    def cursor(self):
        return self.fake_cursor


def test_load_staged_exports_copies_each_export_folder():
    connection = FakeConnection()

    load_staged_exports(connection, "FMCG_ANALYTICS", "RAW", "FMCG_ANALYTICS.RAW.S3_STAGE")

    statements = connection.fake_cursor.statements
    assert len(statements) == 4
    assert "FROM @FMCG_ANALYTICS.RAW.S3_STAGE/orders/" in statements[0]
    assert "COPY INTO FMCG_ANALYTICS.RAW.pricing" in statements[3]
    assert all("MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE" in sql for sql in statements)
    assert connection.fake_cursor.closed


def test_load_staged_exports_rejects_invalid_identifier():
    with pytest.raises(ValueError, match="database"):
        load_staged_exports(FakeConnection(), "RAW; DROP TABLE orders", "RAW", "S3_STAGE")