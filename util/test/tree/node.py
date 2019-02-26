import copy


class Node:
    def __init__(self, flag=None, value=None, skip=False):
        self.parent = None
        self.children = []
        self.temp_children = []  # temporary holder for children until current iteration is finished
        self.flag = flag
        self.value = value

    def __make_my_child(self, node):
        self.children.append(node)
        node.parent = self

    def collect_leaf_nodes(self, lst):
        if len(self.children) == 0:
            lst.append(self)
        else:
            for child in self.children:
                child.collect_leaf_nodes(lst)

    def add_to_leaves(self, node):
        if len(self.children) == 0:
            self.temp_children.append(copy.deepcopy(node))
        else:
            for child in self.children:
                child.add_to_leaves(node)

    def finalize_iteration(self):
        if len(self.temp_children) != 0:
            for temp_child in self.temp_children:
                self.__make_my_child(temp_child)
            del self.temp_children[:]
        else:
            for child in self.children:
                child.finalize_iteration()

    def get_code(self, command):
        if self.parent is None:
            return command
        elif self.flag is None:
            return self.parent.get_code(command='')
        else:
            new_command = "{} -{} {}".format(command, self.flag, self.value)
            return self.parent.get_code(command=new_command)

    def __str__(self):
        return "flag : {} , value : {}, children : {}".format(self.flag, self.value, self.children)
