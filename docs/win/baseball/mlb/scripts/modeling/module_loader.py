from __future__ import annotations

import importlib.util
import sys
from collections.abc import Callable
from pathlib import Path
from types import ModuleType
from typing import Never


def load_module_from_path(
    name: str,
    path: Path,
    *,
    fail: Callable[[str], Never],
    missing_message: str,
    invalid_spec_message: str,
    add_parent_to_path: bool = False,
) -> ModuleType:
    if not path.exists():
        fail(missing_message.format(path=path))

    module_dir = str(path.parent)
    added_to_path = (
        add_parent_to_path
        and module_dir not in sys.path
    )

    if added_to_path:
        sys.path.insert(0, module_dir)

    try:
        spec = importlib.util.spec_from_file_location(
            name,
            path,
        )

        if spec is None or spec.loader is None:
            fail(invalid_spec_message.format(path=path))

        module = importlib.util.module_from_spec(spec)
        sys.modules[name] = module
        spec.loader.exec_module(module)
        return module
    finally:
        if added_to_path:
            sys.path.remove(module_dir)
