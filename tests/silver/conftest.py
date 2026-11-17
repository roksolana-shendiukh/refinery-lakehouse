from unittest.mock import MagicMock

import pytest
from databricks.labs.dqx.engine import DQEngine


@pytest.fixture(scope="module")
def engine(spark):
    """DQX engine without a workspace: the checks tested here need none."""
    return DQEngine(MagicMock(), spark=spark)
