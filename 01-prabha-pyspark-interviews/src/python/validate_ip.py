import ipaddress


ipaddress_ip = "1.23.53.1"


def check_ip_validation(ipaddress):
    # Correct syntax for splitting an IP address
    four_digits = ipaddress.split('.')


    # Ensure there are exactly four parts
    if len(four_digits) != 4:
        return False

    for digit in four_digits:
        try:
            # Convert to integer and check range
            if not (0 <= int(digit) <= 255):
                return False
        except ValueError:  # Catch non-numeric values
            return False

    return True  # If all checks pass, return True


valid_ip: bool = check_ip_validation(ipaddress_ip)

print(valid_ip)


def check_ip_validation2(ipaddress_str):
    """
    Checks if a given string is a valid IPv4 address.

    Args:
        ipaddress_str: The string to check.

    Returns:
        True if the string is a valid IPv4 address, False otherwise.
    """
    try:
        ipaddress.IPv4Address(ipaddress_str)
        return True
    except Exception as e:
        print(e)
        return False

valid_ip2: bool = check_ip_validation2(ipaddress)

print(valid_ip2)