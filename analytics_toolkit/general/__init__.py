from .logging import time_print
from . import read_file as _read_file_module

here = _read_file_module.here
read_file = _read_file_module.read_file
# Preserve the public function export while keeping monkeypatch dotted paths that
# traverse analytics_toolkit.general.read_file.inspect working.
read_file.inspect = _read_file_module.inspect

__all__ = ["here", "read_file", "time_print"]
