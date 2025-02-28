# 3. Longest Substring Without Repeating Characters
# Medium
# Topics
# Companies
# Hint
# Given a string s, find the length of the longest substring without duplicate characters.


# Example 1:

# Input: s = "abcabcbb"
# Output: 3
# Explanation: The answer is "abc", with the length of 3.
# Example 2:

# Input: s = "bbbbb"
# Output: 1
# Explanation: The answer is "b", with the length of 1.
# Example 3:

# Input: s = "pwwkew"
# Output: 3
# Explanation: The answer is "wke", with the length of 3.
# Notice that the answer must be a substring, "pwke" is a subsequence and not a substring.
 

# Constraints:

# 0 <= s.length <= 5 * 104
# s consists of English letters, digits, symbols and spaces.


def Longest_Substring_Without_Repeating_Characters(text: str) -> int:
    """
    Finds the length of the longest substring without repeating characters.

    Args:
        text: The input string.

    Returns:
        The length of the longest substring without repeating characters.
    """
    n = len(text)
    if n == 0:
        return 0

    max_length = 0
    start = 0
    char_index_map = {}  # Dictionary to store the most recent index of each character

    for end in range(n):
        current_char = text[end]

        if current_char in char_index_map and start <= char_index_map[current_char]:
            # If the character is repeated within the current window
            start = char_index_map[current_char] + 1
        else:
            # If the character is not repeated or is outside the current window
            max_length = max(max_length, end - start + 1)

        char_index_map[current_char] = end  # Update the most recent index

    return max_length


print(Longest_Substring_Without_Repeating_Characters("abcabcbb"))  # Output: 3
print(Longest_Substring_Without_Repeating_Characters("bbbbb"))  # Output: 1
print(Longest_Substring_Without_Repeating_Characters("pwwkew"))  # Output: 3
print(Longest_Substring_Without_Repeating_Characters("")) #Output: 0
print(Longest_Substring_Without_Repeating_Characters(" ")) #Output: 1
print(Longest_Substring_Without_Repeating_Characters("au")) #Output: 2
print(Longest_Substring_Without_Repeating_Characters("dvdf")) # Output: 3



# Corrected Solution (Sliding Window Approach):

# The corrected code uses the "sliding window" technique, which is the standard approach for this problem. Here's a breakdown:

# Initialization:

# n = len(text): Gets the length of the string.
# max_length = 0: Initializes the maximum length found so far.
# start = 0: Initializes the start of the sliding window.
# char_index_map = {}: A dictionary to store each character and its most recently seen index.
# Sliding Window Iteration:

# for end in range(n):: The end pointer iterates through the string, expanding the window.
# current_char = text[end]: Gets the character at the end position.
# Checking for Repeating Characters:

# if current_char in char_index_map and start <= char_index_map[current_char]:: This is the core logic. It checks:
# current_char in char_index_map: If the character has been seen before.
# start <= char_index_map[current_char]: If the character's last seen index is within the current window (i.e., after or at the current start).
# If both conditions are true, it means a repeating character is found within the current window.
# start = char_index_map[current_char] + 1: The start pointer is moved to one position after the previous occurrence of the repeating character. This effectively shrinks the window from the left to exclude the repeating character.
# Updating max_length:

# else:: If the character is not a repeat within the current window:
# max_length = max(max_length, end - start + 1): The current window's length (end - start + 1) is compared with the current max_length, and max_length is updated if the current window is longer.
# Updating the char_index_map:

# char_index_map[current_char] = end: The map is updated with the characters current end index
# Return max_length:

# After the loop, max_length holds the length of the longest substring without repeating characters.
# Key Improvements:

# Sliding Window: The corrected code properly implements the sliding window technique.
# Correct Logic: It accurately identifies repeating characters within the current window.
# Efficiency: The time complexity is O(n) because each character is visited at most twice.
# Correctness: It passes all the test cases (including edge cases).
# Added test cases: I added some more test cases to check the code.
# Clarity: I added more comments to explain the code.
