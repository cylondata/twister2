from tree.node import Node


def handle_fixed_arg(arg, root: Node):
    arg_id = arg['id']
    optional = arg['optional']
    val = arg['values']['value']

    new_node = Node(arg_id, val)
    root.add_to_leaves(new_node)

    if optional:
        optional_node = Node(None, None, True)
        root.add_to_leaves(optional_node)

    root.finalize_iteration()
