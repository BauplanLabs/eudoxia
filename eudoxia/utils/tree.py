import uuid
from typing import Generic, TypeVar, Optional, List
'''
This file implements a general Tree data structure. Each Tree is composed of
Nodes, so all Tree node types must sublcass Node in order to ensure certain
fields are present. The Tree is a simple tree structure with helper functions to
search, add nodes to build the tree, and provides a consumable tree iterator to
convert to lists of Segments or Operators
'''
class Node: 
    def __init__(self):
        self.id = uuid.uuid4()
        self.complete = False
        self.children = []
        self.parents = []

    def can_node_execute(self) -> bool: 
        parent_vals = [p.complete for p in self.parents]
        return all(parent_vals)


# TODO: overly simple tree structure cannot express full dag options
# TODO: dependencies could make this messier
class Tree[T: Node]:
    def __init__(self):
        self.tree_id = uuid.uuid4()
        self.node_ids = [] # list of ids in the tree
        self.root = None
        self.iter = None

    def __iter__(self):
        self.iter = TreeIterator(self)
        return self.iter

    def add_node(self, node: T, parents: List[T] = None): 
        assert node.id not in self.node_ids, "Node already in Tree"
        if parents: 
            for parent in parents:
                assert parent.id in self.node_ids, "Parent not in Tree"
                parent.children.append(node)
                node.parents.append(parent)
        else:
            assert self.root is None, "Root already set; needs a parent"
            self.root = node
        self.node_ids.append(node.id)

    def find_node_by_id(self, n_id: uuid.UUID) -> Optional[T]:
        if n_id not in self.node_ids:
            return None

        assert self.root is not None
        stack = []
        stack.append(self.root)
        while len(stack) != 0:
            curr = stack.pop()
            if curr.id == n_id:
                return curr
            stack.extend(curr.children)

        raise RuntimeError("Node id in metadata for tree but node not found")

# TODO: could implement reset/rewind here
class TreeIterator[T: Node]:
    """
    Requires that it be provided with a tree on initialization to iterate over. 
    """
    def __init__(self, tree: Tree[T]):
        self.tree = tree
        self.queue = [self.tree.root]

    def __iter__(self):
        return self

    def __next__(self):
        if len(self.queue) != 0:
            curr = self.queue.pop(0)
            if curr.children:
                self.queue.extend(curr.children)
            return curr
        raise StopIteration


