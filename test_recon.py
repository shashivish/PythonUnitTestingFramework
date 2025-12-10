# tests/test_recon_e2e.py
import pytest
from unittest.mock import patch
from recon.recon_runner import run_recon


@pytest.mark.parametrize("metadata_db", ["scenario1.sql"], indirect=True)
def test_recon_end_to_end(metadata_db, mock_oracle, patch_metadata_repo):

    # Patch Oracle connection globally
    with patch("recon.db_repo.get_oracle_connection", return_value=mock_oracle):

        result = run_recon("RCON_JOB_1")

        assert len(result) == 2
        assert result[0]["status"] == "PASS"
        assert result[1]["status"] == "PASS"
        assert result[0]["src"] == 100
        assert result[0]["tgt"] == 100
