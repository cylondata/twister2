import json
import os
import subprocess

from tree.node import Node
from util.file_iterator import process_directory
from values.array import handle_array_arg
from values.steps import handle_step_arg

value_handlers = {
    "steps": handle_step_arg,
    "array": handle_array_arg
}

process_directory('/home/chathura/Code/twister2/util/test')

with open('base.json') as f:
    data = json.load(f)

jar_root_dir = data['jarRootDir']
t2_bin = data['t2Bin']

for test in data['tests']:
    test_id = test['id']
    jar_dir = jar_root_dir
    if test['directory']['relativeToRoot']:
        jar_dir += test['directory']['path']
    else:
        jar_dir = test['directory']['path']

    jar = os.path.join(jar_dir, test['jar'])

    class_name = test['className']

    root_node = Node()

    for arg in test['args']:
        value_type = arg['values']['type']
        if value_type in value_handlers:
            value_handlers[value_type](arg, root_node)
            root_node.finalize_iteration()

    leaf_nodes = []
    root_node.collect_leaf_nodes(leaf_nodes)

    existing_env = os.environ.copy()
    existing_env["JAVA_HOME"] = "/opt/jdk"

    command = ""
    for leaf_node in leaf_nodes:
        command = leaf_node.get_code("")
        args = [t2_bin, "submit", "standalone", "jar", jar, class_name]
        args.extend(command.strip().split(" "))
        print("Running twister2 job with following args...")
        print(args)
        subprocess.run(args, env=existing_env)

# exec("def hello(arg1):\n\tprint(arg1)")
# eval("hello('Yoho')")
# print(data['tests'][0])
