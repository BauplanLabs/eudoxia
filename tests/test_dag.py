import pytest
from eudoxia.utils.dag import Node, DAG, DAGDependencyTracker


def test_dag_add_node():
    """Test basic node addition to DAG"""
    dag = DAG()
    
    # Create nodes
    node1 = Node()
    node2 = Node()
    node3 = Node()
    
    # Add root node
    dag.add_node(node1)
    assert len(dag.node_ids) == 1
    assert len(dag.roots) == 1
    assert dag.roots[0] == node1
    
    # Add child node
    dag.add_node(node2, [node1])
    assert len(dag.node_ids) == 2
    assert len(dag.roots) == 1  # Still only one root
    assert node2 in node1.children
    assert node1 in node2.parents
    
    # Add another root
    dag.add_node(node3)
    assert len(dag.node_ids) == 3
    assert len(dag.roots) == 2  # Now two roots
    assert node3 in dag.roots


def test_dag_find_node_by_id():
    """Test node lookup by ID"""
    dag = DAG()
    
    node1 = Node()
    node2 = Node()
    
    dag.add_node(node1)
    dag.add_node(node2, [node1])
    
    # Test successful lookup
    found1 = dag.find_node_by_id(node1.id)
    found2 = dag.find_node_by_id(node2.id)
    
    assert found1 == node1
    assert found2 == node2
    
    # Test lookup of non-existent node
    import uuid
    fake_id = uuid.uuid4()
    assert dag.find_node_by_id(fake_id) is None


def test_dag_topological_iteration():
    """Test that iteration respects topological order"""
    dag = DAG()
    
    # Create a simple DAG: A -> B -> D, C -> D
    node_a = Node()
    node_b = Node() 
    node_c = Node()
    node_d = Node()
    
    # Add nodes (parents must be added before children)
    dag.add_node(node_a)  # Root 1
    dag.add_node(node_c)  # Root 2
    dag.add_node(node_b, [node_a])  # Dependent on A
    dag.add_node(node_d, [node_b, node_c])  # Dependent on B and C
    
    # Iterate and track order
    iteration_order = list(dag)
    
    # Verify topological constraints
    a_pos = iteration_order.index(node_a)
    b_pos = iteration_order.index(node_b)  
    c_pos = iteration_order.index(node_c)
    d_pos = iteration_order.index(node_d)
    
    # A must come before B
    assert a_pos < b_pos, "A should come before B"
    
    # Both B and C must come before D
    assert b_pos < d_pos, "B should come before D"
    assert c_pos < d_pos, "C should come before D"
    
    # All nodes should be returned
    assert len(iteration_order) == 4


def test_dag_multiple_parents():
    """Test that nodes can have multiple parents"""
    dag = DAG()
    
    # Create diamond pattern: A -> B, A -> C, B -> D, C -> D
    node_a = Node()
    node_b = Node()
    node_c = Node() 
    node_d = Node()
    
    dag.add_node(node_a)
    dag.add_node(node_b, [node_a])
    dag.add_node(node_c, [node_a]) 
    dag.add_node(node_d, [node_b, node_c])  # Multiple parents
    
    # Verify structure
    assert len(node_d.parents) == 2
    assert node_b in node_d.parents
    assert node_c in node_d.parents
    
    # Verify iteration order respects all dependencies
    iteration_order = list(dag)
    
    a_pos = iteration_order.index(node_a)
    b_pos = iteration_order.index(node_b)
    c_pos = iteration_order.index(node_c)
    d_pos = iteration_order.index(node_d)
    
    # A must come before B and C
    assert a_pos < b_pos
    assert a_pos < c_pos
    
    # Both B and C must come before D
    assert b_pos < d_pos
    assert c_pos < d_pos


def test_dag_dependency_tracker():
    """Test DAGDependencyTracker for tracking node completion state"""
    dag = DAG()

    # Create a simple DAG: A -> B -> C
    node_a = Node()
    node_b = Node()
    node_c = Node()

    dag.add_node(node_a)
    dag.add_node(node_b, [node_a])
    dag.add_node(node_c, [node_b])

    # Create tracker
    tracker = DAGDependencyTracker(dag)

    # Initially, all nodes should be not succeeded
    assert tracker.succeeded[node_a] == False
    assert tracker.succeeded[node_b] == False
    assert tracker.succeeded[node_c] == False

    # Node A (no parents) should be ready
    assert tracker.all_dependencies_satisfied(node_a) == True

    # Node B (parent A not complete) should not be ready
    assert tracker.all_dependencies_satisfied(node_b) == False

    # Node B should be ready if A is in additional_nodes
    assert tracker.all_dependencies_satisfied(node_b, [node_a]) == True

    # Mark A as succeeded
    tracker.mark_success(node_a)
    assert tracker.succeeded[node_a] == True

    # Now B should be ready
    assert tracker.all_dependencies_satisfied(node_b) == True

    # C still not ready
    assert tracker.all_dependencies_satisfied(node_c) == False

    # Mark B as succeeded
    tracker.mark_success(node_b)

    # Now C should be ready
    assert tracker.all_dependencies_satisfied(node_c) == True

    # Test that marking twice fails
    with pytest.raises(AssertionError):
        tracker.mark_success(node_a)