# tests/conftest.py
import pytest
import sqlite3
import os
from unittest.mock import patch, MagicMock
import sqlparse

# ---------------------------------------------------------
# FIXTURE: Load metadata from scenario SQL file into SQLite
# ---------------------------------------------------------
@pytest.fixture
def metadata_db(request):
    scenario_sql = request.param       # from @pytest.mark.parametrize
    conn = sqlite3.connect(":memory:")
    cur = conn.cursor()

    sql_path = os.path.join(os.path.dirname(__file__), "metadata", scenario_sql)
    with open(sql_path, "r") as f:
        cur.executescript(f.read())

    conn.commit()
    return conn


# ---------------------------------------------------------
# FIXTURE: Mock Oracle connection + validate SQL
# ---------------------------------------------------------
@pytest.fixture
def mock_oracle():
    conn = MagicMock()
    cursor = MagicMock()
    conn.cursor.return_value = cursor

    def side_effect(sql, *args, **kwargs):

        # Validate SQL syntax using sqlparse
        parsed = sqlparse.parse(sql)
        if not parsed:
            raise ValueError(f"Invalid SQL syntax: {sql}")

        fake_cursor = MagicMock()

        # return synthetic counts
        if "SRC_TABLE" in sql.upper():
            fake_cursor.fetchall.return_value = [(100,)]
        elif "TGT_TABLE" in sql.upper():
            fake_cursor.fetchall.return_value = [(100,)]
        else:
            fake_cursor.fetchall.return_value = [(0,)]

        return fake_cursor

    cursor.execute.side_effect = side_effect
    return conn


# ---------------------------------------------------------
# Patch metadata_repo.get_metadata to read from SQLite
# ---------------------------------------------------------
@pytest.fixture
def patch_metadata_repo(metadata_db):
    import recon.metadata_repo as mr

    def fake_get_metadata(job_name):
        cur = metadata_db.cursor()
        cur.execute("""
            SELECT RULE_ID, SRC_SQL, TGT_SQL
            FROM metadata_table
            WHERE JOB = ?
        """, (job_name,))
        rows = cur.fetchall()
        return [
            {"rule_id": r[0], "src_sql": r[1], "tgt_sql": r[2]}
            for r in rows
        ]

    with patch.object(mr, "get_metadata", side_effect=fake_get_metadata):
        yield
