# Depth-First Search (DFS)
# Pattern: Explore as far as possible along a branch before backtracking.
# Example Problem: Find all paths from source to destination in a graph.
# Solution Approach:

def allPaths(graph, source, destination):
    result = []
    
    def dfs(current, path):
        if current == destination:
            result.append(path[:])
            return
        
        for neighbor in graph[current]:
            path.append(neighbor)
            dfs(neighbor, path)
            path.pop()
    
    dfs(source, [source])
    return result
