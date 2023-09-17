def find_all_sequences(a: list, key: callable) -> list:
    """
    Find all sequences in the input iterable 'a' according to the provided 'key' function.
    Returns a list of tuples containing the start and end indexes of each sequence.

    For example:
    find_all_sequences([1, 1, 0, 0, 1, 1, 1, 0, 1, 0, 0, 1, 0, 1, 1], lambda x: x == 1) =>
    [(0, 1), (4, 6), (8, 8), (11, 11), (13, 14)]

    :param a: Input sequence. Iterable object.
    :type a: list
    :param key: A lambda-like function used to select values for sequences.
    :type key: callable
    :return: List of tuples representing the start and end indexes of each sequence.
    :rtype: list
    """
    idx = 0
    result = []
    while idx < len(a):
        tmp = []
        if key(a[idx]):
            tmp = [idx, idx]
            while idx < len(a) and key(a[idx]):
                tmp[1] = idx
                idx += 1
            result.append(tuple(tmp))
            continue
        idx += 1
    return result
