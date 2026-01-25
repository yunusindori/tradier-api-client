from __future__ import annotations

import os
import sys
from pathlib import Path
from typing import Any, Optional, Tuple, Dict

import yaml  # pip install pyyaml


# ---------- template preprocessing ----------

def _preprocess_yaml_text(text: str, extra_placeholders: Optional[Dict[str, str]] = None) -> str:
    """
    Replace {{PLACEHOLDER}} tokens with platform-specific values *before* YAML parsing.
    Built-ins:
      {{HOME}}         -> str(Path.home())
      {{separator}}    -> os.sep
      {{APPDATA}}      -> Windows %APPDATA% or "~/.config" fallback
      {{XDG_CONFIG}}   -> $XDG_CONFIG_HOME or "~/.config"
      {{ENV:VAR_NAME}} -> value of environment variable VAR_NAME (empty if missing)
    You can pass extra_placeholders to override or add tokens.
    """
    # base mapping
    appdata = os.environ.get("APPDATA") or str(Path.home() / ".config")
    xdg_config = os.environ.get("XDG_CONFIG_HOME") or str(Path.home() / ".config")
    mapping = {
        "HOME": str(Path.home()),
        "separator": os.sep,
        "APPDATA": appdata,
        "XDG_CONFIG": xdg_config,
    }
    if extra_placeholders:
        mapping.update(extra_placeholders)

    # Fast path if no tokens at all
    if "{{" not in text:
        return text

    # Replace simple {{KEY}} tokens
    for k, v in mapping.items():
        text = text.replace(f"{{{{{k}}}}}", v)

    # Replace {{ENV:NAME}} tokens
    # Simple scan to avoid regex: find all occurrences of {{ENV:...}}
    start = 0
    while True:
        i = text.find("{{ENV:", start)
        if i == -1:
            break
        j = text.find("}}", i + 6)
        if j == -1:
            break  # unmatched; let YAML error out naturally
        env_key = text[i + 6: j].strip()  # after "ENV:"
        env_val = os.environ.get(env_key, "")
        text = text[:i] + env_val + text[j + 2:]
        start = i + len(env_val)

    return text


# ---------- search helpers ----------

def _walk_up_for_file(start: Path, filename: str) -> Optional[Path]:
    start = start.resolve()
    for p in (start, *start.parents):
        cand = p / filename
        if cand.is_file():
            return cand
    return None


def _search_sys_path(filename: str) -> Optional[Path]:
    filename = Path(filename).name
    for entry in sys.path:
        if not entry:
            continue
        p = Path(entry)
        if p.is_dir():
            cand = p / filename
            if cand.is_file():
                return cand
            for root, _, files in os.walk(p):
                if filename in files:
                    return Path(root) / filename
    return None


# ---------- public API ----------

def find_config(
        *,
        filename: str = "config.yml",
        env_var: str = "TRADIER_CLIENT_CONFIG",
        path: Optional[str | Path] = None,
        app_name: str = "tradier",
        extra_placeholders: Optional[Dict[str, str]] = None,
) -> Tuple[Path, dict]:
    """
    Find and load the first config YAML from these locations (in order):
      1) explicit `path=`
      2) env var path `env_var`
      3) cwd -> parents (filename)
      4) user config (~/.config/<app_name>/<filename> or %APPDATA%\<app_name>\)
      5) anywhere on sys.path (recursive)
    Returns (path, data_dict). Preprocesses {{TOKENS}} before parsing.
    """
    # 1) explicit path
    if path:
        p = Path(path).expanduser()
        if not p.is_file():
            raise FileNotFoundError(f"Config not found at explicit path: {p}")
        return p, _read_yaml(p, extra_placeholders)

    # 2) env var path
    env_val = os.getenv(env_var)
    if env_val:
        p = Path(env_val).expanduser()
        if p.is_file():
            return p, _read_yaml(p, extra_placeholders)

    # 3) cwd -> parents
    p = _walk_up_for_file(Path.cwd(), filename)
    if p:
        return p, _read_yaml(p, extra_placeholders)

    # 4) user config dir
    uc = _user_config_candidate(app_name) / filename
    if uc.is_file():
        return uc, _read_yaml(uc, extra_placeholders)

    # 5) sys.path
    sp = _search_sys_path(filename)
    if sp:
        return sp, _read_yaml(sp, extra_placeholders)

    raise FileNotFoundError(
        f"Could not find {filename!r}. Set {env_var} to a file path or pass `path=`."
    )


def _user_config_candidate(app_name: str) -> Path:
    if os.name == "nt":
        base = os.environ.get("APPDATA") or (Path.home() / "AppData" / "Roaming")
        return Path(base) / app_name
    return Path.home() / ".config" / app_name


def _read_yaml(path: Path, extra_placeholders: Optional[Dict[str, str]] = None) -> dict:
    raw = path.read_text(encoding="utf-8")
    pre = _preprocess_yaml_text(raw, extra_placeholders)
    data = yaml.safe_load(pre) or {}
    if not isinstance(data, dict):
        raise ValueError(f"YAML at {path} is not a mapping/object")
    return data


def get_prop(config_or_path: dict | str | Path, dotted_key: str, default=None) -> Any:
    """
    Return the value at `dotted_key` (exact path). Works with a parsed dict or a file path.
    The return can be a scalar, list, or dict (subtree). Raises KeyError if not found.
    """
    if isinstance(config_or_path, (str, Path)):
        cfg = _read_yaml(Path(config_or_path))
    else:
        cfg = config_or_path

    cur: Any = cfg
    for part in dotted_key.split("."):
        if isinstance(cur, dict) and part in cur:
            cur = cur[part]
        elif isinstance(cur, list) and part.isdigit():
            i = int(part)
            if 0 <= i < len(cur):
                cur = cur[i]
            else:
                raise KeyError(f"List index {i} out of range at segment {part!r}")
        else:
            return default
    return cur
