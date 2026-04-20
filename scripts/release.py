"""One-command release helper.

Runs checks, commits, pushes, and executes an AWS-backed deploy command.
Configuration is read from `.env.release` in the project root.
"""

from __future__ import annotations

import argparse
import os
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
DEFAULT_CONFIG = ROOT / ".env.release"


def load_env_file(path: Path) -> dict[str, str]:
    if not path.exists():
        return {}

    data: dict[str, str] = {}
    for raw in path.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        data[key.strip()] = value.strip().strip("'").strip('"')
    return data


def as_bool(value: str | None, *, default: bool = False) -> bool:
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


def run(cmd: list[str], *, cwd: Path = ROOT, env: dict[str, str] | None = None) -> None:
    print(f"$ {' '.join(cmd)}")
    result = subprocess.run(cmd, cwd=str(cwd), env=env, check=False)
    if result.returncode != 0:
        raise SystemExit(result.returncode)


def run_powershell(command: str, *, env: dict[str, str]) -> None:
    if not command.strip():
        return
    cmd = ["powershell", "-NoProfile", "-ExecutionPolicy", "Bypass", "-Command", command]
    run(cmd, env=env)


def command_exists(name: str, *, env: dict[str, str] | None = None) -> bool:
    try:
        result = subprocess.run(
            [name, "--version"],
            cwd=str(ROOT),
            env=env,
            check=False,
            capture_output=True,
            text=True,
        )
    except OSError:
        return False
    return result.returncode == 0


def add_tool_paths(env: dict[str, str]) -> None:
    extra_paths = [
        Path.home() / "AppData" / "Roaming" / "Python" / f"Python{sys.version_info.major}{sys.version_info.minor}" / "Scripts",
        Path("C:/Program Files/Amazon/AWSCLIV2"),
    ]
    path_parts = env.get("PATH", "").split(os.pathsep)
    for p in extra_paths:
        p_str = str(p)
        if p.exists() and p_str not in path_parts:
            path_parts.insert(0, p_str)
    env["PATH"] = os.pathsep.join(path_parts)


def ensure_aws_cli(config: dict[str, str], *, env: dict[str, str]) -> None:
    require = as_bool(config.get("REQUIRE_AWS_CLI"), default=True)
    if not require:
        return

    if command_exists("aws", env=env) or command_exists("aws.cmd", env=env):
        return

    auto_install = as_bool(config.get("AUTO_INSTALL_AWSCLI"), default=True)
    if not auto_install:
        raise SystemExit(
            "AWS CLI is missing. Install manually or set AUTO_INSTALL_AWSCLI=true in .env.release."
        )

    print("AWS CLI not found in PATH. Installing with pip --user fallback...")
    run([sys.executable, "-m", "pip", "install", "--user", "awscli"], env=env)
    add_tool_paths(env)
    if not (command_exists("aws", env=env) or command_exists("aws.cmd", env=env)):
        raise SystemExit("AWS CLI install fallback finished but `aws` is still unavailable in PATH.")


def git_remote_exists(name: str) -> bool:
    result = subprocess.run(
        ["git", "remote", "get-url", name],
        cwd=str(ROOT),
        check=False,
        capture_output=True,
        text=True,
    )
    return result.returncode == 0


def has_staged_changes() -> bool:
    result = subprocess.run(
        ["git", "diff", "--cached", "--quiet"],
        cwd=str(ROOT),
        check=False,
    )
    return result.returncode == 1


def current_branch() -> str:
    result = subprocess.run(
        ["git", "branch", "--show-current"],
        cwd=str(ROOT),
        check=False,
        capture_output=True,
        text=True,
    )
    branch = result.stdout.strip()
    if not branch:
        raise SystemExit("Unable to detect current git branch.")
    return branch


def run_checks(env: dict[str, str]) -> None:
    run([sys.executable, "scripts/codex_smoke.py"], env=env)
    run([sys.executable, "scripts/codex_report.py"], env=env)
    run([sys.executable, "harness.py"], env=env)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run checks, commit, push, and deploy in one command."
    )
    parser.add_argument("-m", "--message", help="Git commit message override.")
    parser.add_argument(
        "--config",
        default=str(DEFAULT_CONFIG),
        help="Path to env config file. Default: .env.release",
    )
    parser.add_argument("--skip-checks", action="store_true", help="Skip smoke/report/harness.")
    parser.add_argument("--no-stage", action="store_true", help="Skip `git add -A`.")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    config_path = Path(args.config).resolve()
    config = load_env_file(config_path)

    env = os.environ.copy()
    env.update(config)
    add_tool_paths(env)
    if config.get("AWS_REGION"):
        env["AWS_REGION"] = config["AWS_REGION"]
        env["AWS_DEFAULT_REGION"] = config["AWS_REGION"]
    if config.get("AWS_PROFILE"):
        env["AWS_PROFILE"] = config["AWS_PROFILE"]

    ensure_aws_cli(config, env=env)

    if not (args.skip_checks or as_bool(config.get("SKIP_CHECKS"), default=False)):
        run_checks(env)

    remote = config.get("GIT_REMOTE", "origin")
    remote_url = config.get("GIT_REMOTE_URL", "")
    if not git_remote_exists(remote):
        if not remote_url:
            raise SystemExit(
                f"Git remote '{remote}' does not exist. Set GIT_REMOTE_URL in .env.release."
            )
        run(["git", "remote", "add", remote, remote_url], env=env)

    branch = config.get("GIT_BRANCH") or current_branch()

    if not args.no_stage:
        run(["git", "add", "-A"], env=env)

    if has_staged_changes():
        commit_message = args.message or config.get("COMMIT_MESSAGE", "").strip()
        if not commit_message:
            raise SystemExit("No staged commit message. Use -m or set COMMIT_MESSAGE in .env.release.")
        run(["git", "commit", "-m", commit_message], env=env)
    else:
        print("No staged changes. Skipping commit.")

    if as_bool(config.get("PUSH_AFTER_COMMIT"), default=True):
        run(["git", "push", "-u", remote, branch], env=env)

    pre_deploy = config.get("PRE_DEPLOY_COMMAND", "")
    if pre_deploy:
        run_powershell(pre_deploy, env=env)

    deploy_command = config.get("DEPLOY_COMMAND", "")
    if not deploy_command:
        raise SystemExit("DEPLOY_COMMAND is required in .env.release.")
    run_powershell(deploy_command, env=env)

    post_deploy = config.get("POST_DEPLOY_COMMAND", "")
    if post_deploy:
        run_powershell(post_deploy, env=env)

    print("Release flow completed.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
