def find_all_sequences(a: list, key: callable = None) -> list:
    """
    Find all sequences in the input iterable 'a' according to the provided 'key' function.

    Returns a list of tuples containing the start and end indices of each sequence that satisfies the condition.

    For example:
    find_all_sequences([1, 1, 0, 0, 1, 1, 1, 0, 1, 0, 0, 1, 0, 1, 1], lambda x: x == 1) =>
    [(0, 1), (4, 6), (8, 8), (11, 11), (13, 14)]

    :param a: Input sequence. Iterable object.
    :type a: list
    :param key: A callable function used to select values for sequences. Defaults to None. If None, returns every sequence in the list.
    :type key: callable
    :return: List of tuples representing the start and end indexes of each sequence.
    :rtype: list
    """
    result = []
    if key is None:
        unique_vals = set(a)
        for val in unique_vals:
            result.extend(find_all_sequences(a, lambda x: x == val))
        result.sort(key=lambda x: x[0])
        return result
    idx = 0
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


if __name__ == "__main__":
    seq = [1, 1, 2, 2, 1, 1, 1, 1, 0, 0, 0, 5, 4, 0, 0, 5, 5, 4, 4]
    # kaus = [1, 2, 0, 4, 5]
    # ffssqq = []
    # for idx, val in enumerate(kaus):
    #     ffssqq.extend(find_all_sequences(seq, lambda x: x == val))
    # ffssqq.sort(key=lambda x: x[0])
    # print(ffssqq)
    tmp = find_all_sequences(seq)
    print(tmp)
