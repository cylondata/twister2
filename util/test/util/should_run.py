def should_run(test_id, tests, parents, tests_to_run):
    if test_id in tests_to_run:
        return True
    elif test_id in tests and "parent" in tests[test_id]:
        return should_run(tests[test_id]["parent"], tests, parents, tests_to_run)
    elif test_id in parents and "parent" in parents[test_id]:
        return should_run(parents[test_id]["parent"], tests, parents, tests_to_run)
    return False
