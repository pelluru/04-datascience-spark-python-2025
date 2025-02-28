# Sliding window patterns
# https://claude.ai/chat/cd216634-dc3f-4a95-aa5e-ab13114913e8

# The sliding window technique is particularly useful for:

# String/array problems involving subarrays or subsequences
# Finding max/min/average in subarrays of specific size
# Problems that can be solved by examining ranges of elements

# Would you like me to explain any specific aspect of sliding window in more detail?

def max_sum_subarray(arr, k):
    """Find maximum sum of any contiguous subarray of size k."""
    n = len(arr)
    if n < k:
        return None
    
    # Calculate sum of first window
    max_sum = current_sum = sum(arr[:k])
    
    # Slide the window
    for i in range(k, n):
        # Add the incoming element and remove the outgoing element
        current_sum = current_sum + arr[i] - arr[i - k]
        max_sum = max(max_sum, current_sum)
        
    return max_sum

# Example
nums = [2, 1, 5, 1, 3, 2]
print(max_sum_subarray(nums, 3))  # Output: 9 (from subarray [5, 1, 3])