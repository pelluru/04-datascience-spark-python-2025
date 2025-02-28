# Backtracking
# Pattern: Build solutions incrementally and abandon a path when it fails to satisfy constraints.
# Example Problem: Generate all permutations of a string.
# Solution Approach:


def permutations(s):
    result = []
    
    def backtrack(curr, remaining):
        if not remaining:
            result.append(curr)
            return
        
        for i in range(len(remaining)):
            backtrack(curr + remaining[i], remaining[:i] + remaining[i+1:])
    
    backtrack("", s)
    return result

print(permutations("abc"))  # Output: ['abc', 'acb', 'bac', 'bca', 'cab', 'cba']