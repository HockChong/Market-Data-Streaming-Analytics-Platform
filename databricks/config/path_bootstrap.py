"""
Path bootstrap for Databricks notebooks and DLT pipelines.

Provides a single source of truth for workspace paths so that every notebook
and DLT file can use a standardized 2-line bootstrap instead of hardcoding
paths independently.

Usage in notebooks/scripts:
    import sys
    sys.path.insert(0, "/Workspace/path/to/databricks/config")
    from path_bootstrap import bootstrap_project_paths
    bootstrap_project_paths()

Usage in DLT/file-based modules:
    import os, sys
    sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "config"))
    from path_bootstrap import bootstrap_project_paths
    bootstrap_project_paths()

After calling bootstrap_project_paths(), you can import from both
`databricks/config/` and `databricks/utils/` without further sys.path changes.
"""

import os
import sys

_config_dir = os.path.dirname(os.path.abspath(__file__))
_utils_dir = os.path.join(os.path.dirname(_config_dir), "utils")


def bootstrap_project_paths():
    """Add config and utils directories to sys.path if not already present."""
    for path in [_config_dir, _utils_dir]:
        if path not in sys.path:
            sys.path.insert(0, path)
