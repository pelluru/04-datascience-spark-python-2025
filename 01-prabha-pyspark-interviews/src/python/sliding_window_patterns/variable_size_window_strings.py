# Sliding window patterns
# https://claude.ai/chat/cd216634-dc3f-4a95-aa5e-ab13114913e8

# The sliding window technique is particularly useful for:

# String/array problems involving subarrays or subsequences
# Finding max/min/average in subarrays of specific size
# Problems that can be solved by examining ranges of elements

# Would you like me to explain any specific aspect of sliding window in more detail?

def longest_substring_without_repeating(s):
    """Find the length of the longest substring without repeating characters."""
    char_set = set()
    max_length = 0
    left = 0
    
    for right in range(len(s)):
        # If character already in set, shrink window from left
        while s[right] in char_set:
            char_set.remove(s[left])
            left += 1
        
        # Add new character to window
        char_set.add(s[right])
        max_length = max(max_length, right - left + 1)
        
    return max_length

# Example
print(longest_substring_without_repeating("abcabcbb"))  # Output: 3 (for "abc")