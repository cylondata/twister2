import json
import os


def merge(child, parent):
    # root level params
    for key in parent:
        if key not in child:
            child[key] = parent[key]

    if "args" not in child:
        child["args"] = []

    arg_ids = {}
    for arg_obj in child["args"]:
        arg_ids[arg_obj["id"]] = True

    for arg_obj in parent["args"]:
        if arg_obj["id"] not in arg_ids:
            child["args"].append(arg_obj)


def build_test(test, tests):
    if "built" in test:
        return test
    elif "parent" in test:
        test_parent = build_test(tests[test["parent"]], tests)
        merge(test, test_parent)

    tests[test["id"]]["built"] = True
    return tests[test["id"]]


def parse_configs(config_files):
    all_tests = {}  # all tests including parents
    runnable_tests = []
    runnable_tests_map = {}
    base = {}

    for file in config_files:
        with open(file) as f:
            data = json.load(f)
            if os.path.basename(file) == "base.json":
                base = data
            else:
                all_tests[data["id"]] = data

    for test_id in all_tests:
        build_test(all_tests[test_id], all_tests)

    parent_tests_ids = {}
    parent_tests = []
    parent_tests_map = {}

    for test_id in all_tests:
        if "parent" in all_tests[test_id]:
            parent_tests_ids[all_tests[test_id]["parent"]] = True

    for test_id in all_tests:
        if test_id not in parent_tests_ids:
            runnable_tests.append(all_tests[test_id])
            runnable_tests_map[test_id] = all_tests[test_id]
        else:
            parent_tests.append(all_tests[test_id])
            parent_tests_map[test_id] = all_tests[test_id]

    return {
        "tests": runnable_tests,
        "base": base,
        "parents": parent_tests,
        "tests_map": runnable_tests_map,
        "parents_map": parent_tests_map
    }
