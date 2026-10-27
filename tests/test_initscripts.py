import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from tracker import initscripts  # noqa: E402

SCRIPT = (Path(__file__).resolve().parents[1] / "fixtures" / "init_scripts" / "install.sh").read_text()


def test_pip_installs_become_dependencies():
    r = initscripts.translate_init_script(SCRIPT)
    assert r["dependencies"] == ["pandas==2.1.4", "requests>=2.31", "great-expectations"]
    assert r["environment_spec"]["dependencies"] == r["dependencies"]


def test_os_packages_env_names_and_other_commands_are_flagged():
    r = initscripts.translate_init_script(SCRIPT)
    assert r["os_packages"] == ["libpq-dev"]
    assert r["env_var_names"] == ["APP_ENV"]
    assert "curl" in r["other_commands"]
    assert r["needs_manual_review"] is True


def test_env_values_are_never_reported():
    r = initscripts.translate_init_script("export SECRET_TOKEN=abc123\n")
    assert "abc123" not in str(r)
    assert r["env_var_names"] == ["SECRET_TOKEN"]


def test_clean_script_needs_no_review():
    r = initscripts.translate_init_script("#!/bin/bash\nset -e\n%pip install --upgrade polars\n")
    assert r["dependencies"] == ["polars"]
    assert r["needs_manual_review"] is False


def test_wheel_path_and_requirements_file_go_to_manual_review():
    r = initscripts.translate_init_script("pip install /dbfs/libs/a.whl -r reqs.txt\n")
    assert r["dependencies"] == []
    assert r["needs_manual_review"] is True
