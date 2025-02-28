# Sliding window patterns
# https://claude.ai/chat/cd216634-dc3f-4a95-aa5e-ab13114913e8

# The sliding window technique is particularly useful for:

# String/array problems involving subarrays or subsequences
# Finding max/min/average in subarrays of specific size
# Problems that can be solved by examining ranges of elements

# Would you like me to explain any specific aspect of sliding window in more detail?

def moving_average(arr, k):
    """Calculate moving averages with window size k."""
    result = []
    window_sum = sum(arr[:k])
    result.append(window_sum / k)
    
    for i in range(k, len(arr)):
        # Remove the element leaving the window
        window_sum -= arr[i - k]
        # Add the new element entering the window
        window_sum += arr[i]
        # Calculate and store the average
        result.append(window_sum / k)
        
    return result

# Example
prices = [1, 3, 5, 7, 9, 11, 13, 15]
print(moving_average(prices, 3))  # 3-day moving average
# Output: [3.0, 5.0, 7.0, 9.0, 11.0, 13.0]