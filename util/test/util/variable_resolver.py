import re

def resolve_variables(leaf, command):
    new_command = command
    while(re.search(r".*\${.+}.*",new_command)):
      new_command = leaf.resolve_variables(new_command)
    return new_command
