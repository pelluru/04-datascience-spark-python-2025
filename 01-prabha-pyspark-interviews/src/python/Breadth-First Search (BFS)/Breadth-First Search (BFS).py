# Breadth-First Search (BFS)
# Pattern: Explore all neighbors at the current depth before moving to nodes at the next depth level.
# Example Problem: Find shortest path in an unweighted graph.
# Solution Approach:

from collections import deque

def shortestPath(graph, start, end):
    queue = deque([(start, [start])])
    visited = set([start])
    
    while queue:
        node, path = queue.popleft()
        
        if node == end:
            return path
        
        for neighbor in graph[node]:
            if neighbor not in visited:
                visited.add(neighbor)
                queue.append((neighbor, path + [neighbor]))
    
    return []

