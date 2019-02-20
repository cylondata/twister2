from tree.node import Node


def handle_step_arg(arg, root: Node):
    arg_id = arg['id']
    optional = arg['optional']
    val_init = arg['values']['from']
    val_end = arg['values']['to']
    val_step = arg['values']['step']

    for val in range(val_init, val_end + 1, val_step):
        new_node = Node(arg_id, val)
        root.add_to_leaves(new_node)

    if optional:
        optional_node = Node(None, None, True)
        root.add_to_leaves(optional_node)

    root.finalize_iteration()
