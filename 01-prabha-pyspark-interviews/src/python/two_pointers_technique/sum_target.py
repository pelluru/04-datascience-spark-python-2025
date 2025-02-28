# https://claude.ai/share/b8e0a4ce-e348-47e9-b8ec-4e30b5b2ff1e
# Two Pointer Technique
# Pattern: Use two pointers to traverse an array or string, often moving in opposite directions or at different speeds.
# Example Problem: Find a pair that sums to a target in a sorted array.
# Solution Approach:

def twoSum(nums, target):
    left, right = 0, len(nums) - 1
    
    while left < right:
        current_sum = nums[left] + nums[right]
        if current_sum == target:
            return [left, right]
        elif current_sum < target:
            left += 1
        else:
            right -= 1
    
    return []

print(twoSum([2, 7, 11, 15], 9))