class ListNode:
    def __init__(self, val=0, next=None):
        self.val = val
        self.next = next

def add_two_numbers(l1, l2):
    dummy_head = ListNode()  # Dummy node to serve as the starting point
    current = dummy_head     # Pointer to construct the new list
    carry = 0                # Initialize carry to 0

    # Traverse both lists until the end of both
    while l1 or l2 or carry:
        val1 = l1.val if l1 else 0  # Get the current value of l1, or 0 if l1 is exhausted
        val2 = l2.val if l2 else 0  # Get the current value of l2, or 0 if l2 is exhausted

        # Calculate the sum and the new carry
        total = val1 + val2 + carry
        carry = total // 10
        new_val = total % 10

        # Create a new node with the computed value
        current.next = ListNode(new_val)
        current = current.next  # Move to the next node

        # Move to the next nodes in l1 and l2, if available
        if l1:
            l1 = l1.next
        if l2:
            l2 = l2.next

    return dummy_head.next  # The result list starts from the next node of dummy_head

# Helper function to create a linked list from a list of digits
def create_linked_list(digits):
    dummy_head = ListNode()
    current = dummy_head
    for digit in digits:
        current.next = ListNode(digit)
        current = current.next
    return dummy_head.next

# Helper function to print the linked list
def print_linked_list(node):
    digits = []
    while node:
        digits.append(str(node.val))
        node = node.next
    print(" -> ".join(digits))

# Create linked lists for [2, 4, 3] and [5, 6, 4]
l1 = create_linked_list([2, 4, 3])
l2 = create_linked_list([5, 6, 4])

# Add the two numbers
result = add_two_numbers(l1, l2)

# Print the result
print_linked_list(result)  # Output: 7 -> 0 -> 8


# To add two numbers represented by linked lists, where each node contains a single digit and the digits are stored in reverse order, we can implement a solution in Python. This approach involves traversing both linked lists, adding corresponding digits along with any carry from the previous addition, and constructing a new linked list to represent the sum.

# Implementation Steps:
# 	1.	Define the Node Class: Each node will store a single digit and a reference to the next node.



# 	Define the Function to Add Two Numbers: This function will take two linked lists as input and return a new linked list representing their sum.

# 	•	Initialization: We use a dummy head node to simplify edge cases and a carry variable to handle sums exceeding 9.
# 	•	Traversal and Addition:
# 	•	We iterate through both linked lists simultaneously.
# 	•	For each pair of nodes, we retrieve their values (defaulting to 0 if a list is shorter) and compute the sum along with the carry.
# 	•	The new digit is the remainder of the sum divided by 10 (total % 10), and the new carry is the integer division of the sum by 10 (total // 10).
# 	•	A new node with the computed digit is appended to the result list.
# 	•	Completion: After processing all nodes and the final carry, the function returns the next node of dummy_head, which is the head of the resultant linked list.

# Example Usage:

# Here’s how you can create linked lists for the numbers 342 and 465, add them using the add_two_numbers function, and print the result:


# For a visual explanation and further insights, you might find this video tutorial helpful:

# Add Two Numbers - Leetcode 2 - Python

# This video provides a step-by-step walkthrough of the problem and its solution in Python.