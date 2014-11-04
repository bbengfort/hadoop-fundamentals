def isprime(num):
    n = abs(int(num))         # n is positive
    if n < 2: return False    # 0 and 1 are not prime
    if n == 2: return True    # 2 is the only even prime
    if not n & 1:             # All even nums are not
        return False
    for x in xrange(3, int(n**0.5)+1, 2):
        if n % x == 0:
            return False
    return True
