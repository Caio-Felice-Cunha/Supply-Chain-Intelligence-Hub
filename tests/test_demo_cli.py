import json
import subprocess
import sys
import zipfile


def test_smoke_cli_publishes_dashboard_report_and_verifiable_archive(tmp_path):
    output = tmp_path / "site"
    completed = subprocess.run(
        [
            sys.executable,
            "-m",
            "scripts.orchestration.demo",
            "--scale",
            "smoke",
            "--seed",
            "42",
            "--output",
            str(output),
            "--database",
            "skip",
        ],
        check=True,
        capture_output=True,
        text=True,
    )
    assert '"state": "PASS"' in completed.stdout
    dashboard = json.loads((output / "data" / "dashboard.json").read_text())
    manifest = json.loads((output / "data" / "manifest.json").read_text())
    assert dashboard["quality"]["failed"] == 0
    assert dashboard["meta"]["source_rows"] == sum(get_scale_count for get_scale_count in manifest["source_counts"].values())
    archive = output / "downloads" / "supply-chain-dataset-seed-42.zip"
    with zipfile.ZipFile(archive) as bundle:
        assert "manifest.json" in bundle.namelist()
        assert len([name for name in bundle.namelist() if name.endswith(".csv")]) == 7
    assert (output / "report" / "index.html").exists()
    page = (output / "index.html").read_text(encoding="utf-8")
    assert 'href="accessibility.css"' in page
    assert '<pre tabindex="0">' in page
    assert (output / "accessibility.css").read_text(encoding="utf-8")
