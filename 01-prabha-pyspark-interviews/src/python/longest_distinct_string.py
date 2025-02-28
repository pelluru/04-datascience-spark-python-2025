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
