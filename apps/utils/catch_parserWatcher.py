from __future__ import annotations
import importlib.util
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Callable, Dict, Optional
import yaml
import time


@dataclass
class LoadedParser:
    module_name: str           # e.g., "hri485_pulse"
    path: Path                 # filesystem path to module .py
    mtime: float               # last modified time
    func: Callable             # callable(payload_dict) -> List[dict]


class DynamicParserManager:
    """
    Hot-reload parser manager that:
      - polls configs/device_profile/profiles.yml every N seconds
      - dynamically imports modules under apps/utils/parsers/<module>.py
      - reloads a module when its file mtime changes
    Usage:
      pm = DynamicParserManager(repo_root, interval_sec=10)
      parser = pm.get_parser("HRI485_with_pulse_counter")
      if parser: rows = parser(payload)
    """

    def __init__(self, repo_root: Path, interval_sec: float = 10.0):
        self.repo_root = Path(repo_root)
        self.interval = interval_sec
        self.profiles_path = self.repo_root / "configs" / "device_profile" / "profiles.yml"
        self.parsers_dir = self.repo_root / "apps" / "utils" / "parsers"
        self._profile_map: Dict[str, str] = {}        # profile_name -> module_name
        self._profile_mtime: float = 0.0
        self._loaded: Dict[str, LoadedParser] = {}    # module_name -> LoadedParser

    # ------------ public API ------------

    def get_parser(self, device_profile_name: str) -> Optional[Callable]:
        """
        Return a callable parser for given deviceProfileName, or None if not configured.
        This method is safe to call on every message; it throttles reload checks by interval.
        """
        self._maybe_reload_profiles()
        module_name = self._profile_map.get(device_profile_name)
        if not module_name:
            return None
        return self._ensure_module_loaded(module_name)

    # ------------ internal helpers ------------

    def _maybe_reload_profiles(self):
        """Reload profiles.yml if changed or if interval elapsed since last check."""
        try:
            mtime = self.profiles_path.stat().st_mtime
        except FileNotFoundError:
            mtime = 0.0

        # Check both mtime and interval to avoid excessive IO
        if mtime != self._profile_mtime:
            self._load_profiles()

    def _load_profiles(self):
        """Load YAML mapping: profile -> module."""
        self._profile_map = {}
        if not self.profiles_path.exists():
            self._profile_mtime = 0.0
            return
        with open(self.profiles_path, "r", encoding="utf-8") as f:
            data = yaml.safe_load(f) or {}
        self._profile_map = (data.get("profiles") or {})
        self._profile_mtime = self.profiles_path.stat().st_mtime

    def _ensure_module_loaded(self, module_name: str) -> Optional[Callable]:
        """
        Import or reload the specific parser module by name (file under parsers_dir).
        The module must define a top-level function named 'parse'.
        """
        mod_path = self.parsers_dir / f"{module_name}.py"
        if not mod_path.exists():
            # module file missing
            self._loaded.pop(module_name, None)
            return None

        mtime = mod_path.stat().st_mtime
        lp = self._loaded.get(module_name)

        if lp and abs(lp.mtime - mtime) < 1e-6:
            # up to date
            return lp.func

        # (re)load module from file location
        spec = importlib.util.spec_from_file_location(module_name, mod_path)
        if spec is None or spec.loader is None:
            return None
        module = importlib.util.module_from_spec(spec)
        # ensure import machinery can reference it
        sys.modules[module_name] = module
        spec.loader.exec_module(module)  # type: ignore

        # retrieve required function
        func = getattr(module, "parse", None)
        if not callable(func):
            # invalid parser module (no parse function)
            self._loaded.pop(module_name, None)
            return None

        self._loaded[module_name] = LoadedParser(
            module_name=module_name, path=mod_path, mtime=mtime, func=func
        )
        return func
