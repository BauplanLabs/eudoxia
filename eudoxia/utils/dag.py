import uuid
from typing import Generic, TypeVar, Optional, List
'''
This file implements a general DAG (Directed Acyclic Graph) data structure. Each DAG is composed of
Nodes, so all DAG node types must subclass Node in order to ensure certain
fields are present. The DAG supports multiple roots and provides a topological iterator to
convert to lists of Segments or Operators in dependency order.
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


class DAG[T: Node]:
    def __init__(self):
        self.dag_id = uuid.uuid4()
        self.node_ids = [] # list of ids in the DAG
        self.node_lookup = {} # dict mapping node_id -> node for O(1) lookup
        self.roots = [] # list of root nodes (nodes with no parents)
        self.iter = None

    def __len__(self):
        return len(self.node_ids)

    def __iter__(self):
        self.iter = DAGIterator(self)
        return self.iter

    def add_node(self, node: T, parents: List[T] = None): 
        assert node.id not in self.node_ids, "Node already in DAG"
        if parents: 
            for parent in parents:
                assert parent.id in self.node_ids, "Parent not in DAG"
                parent.children.append(node)
                node.parents.append(parent)
        else:
            # Node has no parents, so it's a root
            self.roots.append(node)
        self.node_ids.append(node.id)
        self.node_lookup[node.id] = node

    def find_node_by_id(self, n_id: uuid.UUID) -> Optional[T]:
        return self.node_lookup.get(n_id)


class DAGIterator[T: Node]:
    """
    Iterates over nodes in topological order - no node is returned until 
    all its parents have been returned.
    """
    def __init__(self, dag: DAG[T]):
        self.dag = dag
        self.returned = set()  # Track which nodes we've already returned
        # TODO: dequeue for queue?
        self.queue = list(self.dag.roots)  # Start with all roots

    def __iter__(self):
        return self

    def __next__(self):
        if not self.queue:
            raise StopIteration

        curr = self.queue.pop(0)
        self.returned.add(curr.id)

        # Check each child to see if it's now ready
        for child in curr.children:
            if child.id not in self.returned and all(parent.id in self.returned for parent in child.parents):
                self.queue.append(child)

        return curr
