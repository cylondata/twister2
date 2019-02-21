from tree.node import Node


def handle_none_arg(arg, root: Node):
    arg_id = arg['id']
    optional = arg['optional']

    new_node = Node(arg_id, '')
    root.add_to_leaves(new_node)

    if optional:
        optional_node = Node(None, None, True)
        root.add_to_leaves(optional_node)

    root.finalize_iteration()
