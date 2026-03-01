import os

os.environ["AUTH_ENABLED"] = "false"

from fau_bot_core.service import SERVICE  # noqa: E402


def test_fau_core_dev_collaboration_scan_payload():
    out = SERVICE.run_dev_collaboration_scan(
        source_root=".",
        triggered_by="pytest",
        max_files=60,
        max_file_size_kb=256,
    )
    assert int(out.get("scanned_files") or 0) > 0
    assert isinstance(out.get("totals"), dict)
    assert "hitl_created" in out
    assert "suggestions_generated" in out

