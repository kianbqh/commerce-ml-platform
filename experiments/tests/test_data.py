import pytest

from abtest import data


def test_checked_dataset_has_expected_randomized_arms_and_binary_outcomes():
    df = data.load()
    assert len(df) == 64000 and set(df.arm) == {"control", "mens", "womens"}
    assert not df.isna().any().any()
    assert df.visit.isin([0, 1]).all() and df.conversion.isin([0, 1]).all()
    assert (df.spend >= 0).all()


def test_checksum_mismatch_is_rejected(tmp_path, monkeypatch):
    bad = tmp_path / "bad.csv.gz"
    bad.write_bytes(b"altered dataset")
    monkeypatch.setattr(data, "PATH", bad)
    with pytest.raises(ValueError, match="checksum mismatch"):
        data.load()
