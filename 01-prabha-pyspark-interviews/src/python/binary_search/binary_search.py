# Binary Search
# Pattern: Divide-and-conquer approach for sorted arrays to achieve O(log n) time complexity.
# Example Problem: Find the first occurrence of a target in a sorted array.
# Solution Approach:


def firstOccurrence(nums, target):
    left, right = 0, len(nums) - 1
    result = -1
    
    while left <= right:
        mid = (left + right) // 2
        
        if nums[mid] == target:
            result = mid
            right = mid - 1  # Continue searching left half
        elif nums[mid] < target:
            left = mid + 1
        else:
            right = mid - 1
    
    return result

print(firstOccurrence([5, 7, 7, 8, 8, 10], 8))

