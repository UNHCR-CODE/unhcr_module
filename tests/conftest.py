import pytest

def pytest_addoption(parser):
    """Registers custom command-line arguments for tests."""
    parser.addoption("--env", action="store", default=None, help="Path to environment file")
    parser.addoption("--log", action="store", default="INFO", help="Logging level")

@pytest.fixture(scope="session")
def env_file(request):
    """Fixture to access the --env argument."""
    return request.config.getoption("--env")

@pytest.fixture(scope="session")
def log_level(request):
    """Fixture to access the --log argument."""
    return request.config.getoption("--log")
