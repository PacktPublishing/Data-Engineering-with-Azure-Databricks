import os

import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark():
    """Get the active SparkSession (works on Databricks serverless and via Connect)."""
    try:
        session = SparkSession.getActiveSession()
        if session is not None:
            yield session
            return
    except Exception:
        pass

    from databricks.connect import DatabricksSession
    session = DatabricksSession.builder.serverless().getOrCreate()
    yield session


@pytest.fixture(scope="session")
def catalog():
    """Resolve the target catalog from the environment."""
    target = os.environ.get("BUNDLE_TARGET", "staging")
    return f"analytics_{target}"
