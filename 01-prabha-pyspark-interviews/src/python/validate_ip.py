ipaddress = "1.23.53.1"


def check_ip_validation(ipaddress):
    # Correct syntax for splitting an IP address
    four_digits = ipaddress.split(",")

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


valid_ip: bool = check_ip_validation(ipaddress)

print(valid_ip)
