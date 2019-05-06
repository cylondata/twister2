import json
import os
import subprocess
import base64
import sys

from tree.node import Node
from util.config_parser import parse_configs
from util.file_iterator import process_directory
from util.variable_resolver import resolve_variables
from util.should_run import should_run
from values.array import handle_array_arg
from values.fixed import handle_fixed_arg
from values.none import handle_none_arg
from values.steps import handle_step_arg

value_handlers = {
    "steps": handle_step_arg,
    "array": handle_array_arg,
    "none": handle_none_arg,
    "fixed": handle_fixed_arg
}

config_files = []
process_directory(os.path.abspath("./tests"), config_files)

configs = parse_configs(config_files)

jar_root_dir = configs["base"]['jarRootDir']
t2_bin = configs["base"]['t2Bin']
jdk_dir = configs["base"]["jdk"]

partial_run = len(sys.argv) > 1
tests_to_run = []
if partial_run:
    tests_to_run = str(sys.argv[1]).split(",")

for test in configs['tests']:
    test_id = test['id']
    if partial_run and not should_run(test_id, configs['tests_map'], configs['parents_map'],
                                      tests_to_run):
        print("skipping.." + test_id)
        continue

    jar_dir = jar_root_dir
    if test['directory']['relativeToRoot']:
        jar_dir += test['directory']['path']
    else:
        jar_dir = test['directory']['path']

    jar = os.path.join(jar_dir, test['jar'])

    class_name = test['className']

    meta = {
        "id": test_id,
        "resultsFile": test['resultsFile'],
        "args": []
    }  # benchmark metadata

    root_node = Node()

    for arg in test['args']:
        if "omitInCSV" not in arg:
            meta["args"].append({
                "arg": arg['id'],  # arg value can be read in java Config with this id
                "column": arg['name']
            })
        value_type = arg['values']['type']
        if value_type in value_handlers:
            value_handlers[value_type](arg, root_node)
            root_node.finalize_iteration()

    leaf_nodes = []
    root_node.collect_leaf_nodes(leaf_nodes)

    existing_env = os.environ.copy()
    existing_env["JAVA_HOME"] = jdk_dir

    command = ""
    for leaf_node in leaf_nodes:
        command = leaf_node.get_code("")
        command = resolve_variables(leaf_node, command)
        args = [t2_bin, "submit", "standalone", "jar", jar, class_name]
        args.extend(command.strip().split(" "))
        args.extend(["-bmeta", base64.b64encode(json.dumps(meta).encode("utf-8"))])
        print("\nRunning twister2 job with following args...")
        print(args)
        # subprocess.run(args, env=existing_env)
        p = subprocess.Popen(args, stdout=subprocess.PIPE, bufsize=1, env=existing_env)
        for line in iter(p.stdout.readline, b''):
            print(line)
        p.stdout.close()
        p.wait()
