import os
from pathlib import Path

os.environ["AUTH_ENABLED"] = "false"

from fau_bot_core.service import SERVICE  # noqa: E402


def test_architect_rules_available():
    rules = SERVICE.architect_rules()
    assert rules
    assert any(r.get("tier") == "P0" for r in rules)
    assert any(r.get("rule_id") == "P0-003" for r in rules)


def test_architect_scan_finds_basic_issues(tmp_path: Path):
    sample = tmp_path / "bad_sample.py"
    sample.write_text(
        "\n".join(
            [
                "def f(x=[]):",
                "    try:",
                "        eval('print(1)')",
                "    except:",
                "        pass",
                "    print('debug')",
            ]
        ),
        encoding="utf-8",
    )

    out = SERVICE.run_architect_scan(
        source_root=str(tmp_path),
        triggered_by="pytest",
        max_files=30,
        max_file_size_kb=128,
    )
    assert int(out.get("scanned_files") or 0) >= 1
    summary = out.get("summary") or {}
    assert int(summary.get("total_findings") or 0) >= 1
    assert "hitl_created" in out

