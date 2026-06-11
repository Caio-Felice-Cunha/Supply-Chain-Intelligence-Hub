"""Shared pytest fixtures and path setup.

These tests run fully offline against synthetic DataFrames. They do not need
MySQL, Docker, or any network access, so they can run in CI on a plain Python
environment.
"""

import logging
import os
import sys

import pytest

# Make the scripts/ package tree importable without installing the project.
REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SCRIPTS_DIR = os.path.join(REPO_ROOT, "scripts")
QUALITY_DIR = os.path.join(SCRIPTS_DIR, "quality")
for path in (SCRIPTS_DIR, QUALITY_DIR):
    if path not in sys.path:
        sys.path.insert(0, path)


@pytest.fixture
def null_logger():
    """A logger that discards output, for components that require one."""
    logger = logging.getLogger("test_etl")
    logger.addHandler(logging.NullHandler())
    logger.setLevel(logging.CRITICAL)
    return logger
