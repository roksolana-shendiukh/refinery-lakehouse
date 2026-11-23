import pytest
from assistant.settings import AssistantSettings, SettingsError

REQUIRED = {
    "ASSISTANT_LLM_ENDPOINT": "databricks-meta-llama-3-3-70b-instruct",
    "ASSISTANT_INDEX_NAME": "c.rag.eia_article_chunks_index",
    "ASSISTANT_LOG_TABLE": "c.rag.assistant_requests",
    "DATABRICKS_WAREHOUSE_ID": "abc",
}


@pytest.fixture
def env(monkeypatch):
    for name in ("ASSISTANT_SEARCH_RESULTS", "ASSISTANT_MAX_ITERATIONS"):
        monkeypatch.delenv(name, raising=False)
    for name, value in REQUIRED.items():
        monkeypatch.setenv(name, value)
    return monkeypatch


def test_reads_names_and_defaults(env):
    settings = AssistantSettings.from_env()

    assert settings.llm_endpoint == "databricks-meta-llama-3-3-70b-instruct"
    assert settings.index_name == "c.rag.eia_article_chunks_index"
    assert settings.log_table == "c.rag.assistant_requests"
    assert (settings.search_results, settings.max_iterations) == (5, 3)


def test_reads_limits_from_the_environment(env):
    env.setenv("ASSISTANT_SEARCH_RESULTS", "8")
    env.setenv("ASSISTANT_MAX_ITERATIONS", "2")

    settings = AssistantSettings.from_env()

    assert (settings.search_results, settings.max_iterations) == (8, 2)


@pytest.mark.parametrize("name", list(REQUIRED))
def test_missing_required_setting_is_an_error(env, name):
    env.delenv(name)

    with pytest.raises(SettingsError, match=name):
        AssistantSettings.from_env()


@pytest.mark.parametrize("value", ["0", "-1", "many"])
def test_invalid_limit_is_an_error(env, value):
    env.setenv("ASSISTANT_MAX_ITERATIONS", value)

    with pytest.raises(SettingsError, match="ASSISTANT_MAX_ITERATIONS"):
        AssistantSettings.from_env()
