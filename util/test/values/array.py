from tree.node import Node


def handle_array_arg(arg, root):
    arg_id = arg['id']
    optional = arg['optional']
    val_array = arg['values']['array']

    for val in val_array:
        new_node = Node(arg_id, val)
        root.add_to_leaves(new_node)

    if optional:
        optional_node = Node(None, None, True)
        root.add_to_leaves(optional_node)

    root.finalize_iteration()
