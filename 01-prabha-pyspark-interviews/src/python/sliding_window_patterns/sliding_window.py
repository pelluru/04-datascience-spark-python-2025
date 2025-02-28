# https://claude.ai/share/b8e0a4ce-e348-47e9-b8ec-4e30b5b2ff1e


def maxSubarraySum(nums, k):
    if len(nums) < k:
        return None
    
    # Calculate first window sum
    max_sum = current_sum = sum(nums[:k])
    
    # Slide window and update max
    for i in range(k, len(nums)):
        current_sum = current_sum - nums[i-k] + nums[i]
        max_sum = max(max_sum, current_sum)
    
    return max_sum

print(maxSubarraySum([2, 1, 5, 1, 3, 2], 3))

