# Original source : https://github.com/cnkyrpsgl/leetcode?tab=readme-ov-file

# To solve the problem of finding two indices in an array whose corresponding values sum up to a given target, we can utilize a hash map (dictionary) for an efficient solution. This approach leverages the hash map to store and quickly lookup the complement of each number as we iterate through the array.

# Approach:

# 	1.	Initialize a Hash Map:
# 	•	Create an empty dictionary to store each number’s complement and its index.
# 	2.	Iterate Through the Array:
# 	•	For each number in the array, calculate its complement with respect to the target.
# 	•	Check if this complement is already in the hash map:
# 	•	If it is, we’ve found the two numbers that add up to the target; return their indices.
# 	•	If it’s not, add the current number and its index to the hash map.
# 	3.	Assumptions:
# 	•	Each input has exactly one solution.
# 	•	The same element cannot be used twice.

# Data Structure Pattern:

# 	•	Hash Map (Dictionary):
# 	•	This pattern is used for its efficient O(1) average-time complexity for insert and lookup operations. 
#      By storing each number’s complement as we iterate through the array, 
#      we can quickly determine if the complement exists in the array without needing a nested loop.

# Python Function:

def two_sum(nums, target):
    # Initialize an empty dictionary to store number and its index
    num_to_index = {}

    # Iterate over the list
    for index, num in enumerate(nums):
        # Calculate the complement
        complement = target - num

        # Check if complement is in the dictionary
        if complement in num_to_index:
            # If found, return the indices
            return [num_to_index[complement], index]

        # Otherwise, add the number and its index to the dictionary
        num_to_index[num] = index

    # If no solution is found, return an empty list (though the problem guarantees one solution)
    return []


	# •	Initialization: We start by creating an empty dictionary num_to_index to store numbers and their corresponding indices as we iterate through the list.
	# •	Iteration: For each number in the list:
	# •	Compute its complement by subtracting the number from the target.
	# •	Check if this complement is already a key in the dictionary:
	# •	If it is, this means we’ve previously encountered the complement, and together with the current number, they sum up to the target. We then return the index of the complement (retrieved from the dictionary) and the current index.
	# •	If it’s not, we add the current number and its index to the dictionary for future reference.
	# •	Assumptions Handling: The problem guarantees exactly one solution, so we don’t need to handle cases with no solutions or multiple solutions.

    # Example 1
nums1 = [2, 7, 11, 15]
target1 = 9
print(two_sum(nums1, target1))  # Output: [0, 1]

# Example 2
nums2 = [3, 2, 4]
target2 = 6
print(two_sum(nums2, target2))  # Output: [1, 2]

# Example 3
nums3 = [3, 3]
target3 = 6
print(two_sum(nums3, target3))  # Output: [0, 1]

# In Example 1, the numbers at indices 0 and 1 (2 + 7) sum up to the target 9. In Example 2, the numbers at indices 1 and 2 (2 + 4) sum up to 6. In Example 3, the numbers at indices 0 and 1 (3 + 3) sum up to 6.

# This solution efficiently finds the required indices with a time complexity of O(n), where n is the number of elements in the array, by utilizing the hash map for constant-time lookups.