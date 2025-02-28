# Dynamic Programming
# Pattern: Break complex problems into simpler subproblems and store their solutions.
# Example Problem: Find the maximum sum increasing subsequence.
# Solution Approach:
def maxSumIncreasingSubsequence(nums):
    n = len(nums)
    dp = nums[:]  # Initialize dp with values from nums
    
    for i in range(1, n):
        for j in range(i):
            if nums[i] > nums[j]:
                dp[i] = max(dp[i], dp[j] + nums[i])
    
    return max(dp)

print(maxSumIncreasingSubsequence([1, 101, 2, 3, 100, 4, 5]))   