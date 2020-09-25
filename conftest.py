def pytest_ignore_collect(path):
    if str(path).endswith("smoke_test.py"):
        return True