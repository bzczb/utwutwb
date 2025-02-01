import typing as T
from array import array

from cykhash import Int64Set

from utwutwb.constants import ARR_TYPE, ARRAY_SIZE_MAX, SET_SIZE_MIN

EfficientSet: T.TypeAlias = T.Union[Int64Set, array, int, None]


def create(a: T.Iterable[int]) -> EfficientSet:
    ls = list(a)
    if len(ls) == 0:
        return None
    elif len(ls) == 1:
        return ls[0]
    elif len(ls) <= ARRAY_SIZE_MAX:
        return array(ARR_TYPE, ls)
    else:
        return Int64Set(ls)


def to_set(a: EfficientSet) -> Int64Set:
    if a is None:
        return Int64Set()
    elif type(a) == int:  # noqa
        return Int64Set([a])
    elif type(a) == array:  # noqa
        return Int64Set(a)
    else:
        assert type(a) == Int64Set  # noqa
        return a


def from_set(a: Int64Set) -> EfficientSet:
    if len(a) == 0:
        return None
    elif len(a) == 1:
        return next(iter(a))
    elif len(a) <= ARRAY_SIZE_MAX:
        arr = array(ARR_TYPE)
        arr.extend(a)
        return arr
    else:
        return a


def size(a: EfficientSet) -> int:
    if a is None:
        return 0
    elif type(a) == int:  # noqa
        return 1
    elif type(a) == array:  # noqa
        return len(a)
    else:
        assert type(a) == Int64Set  # noqa
        return len(a)


def iterate(a: EfficientSet) -> T.Iterator[int]:
    if a is None:
        yield from ()
    elif type(a) == int:  # noqa
        yield a
    else:
        yield from a  # type: ignore


def copy(a: EfficientSet) -> EfficientSet:
    if a is None:
        return None
    elif type(a) == int:  # noqa
        return a
    elif type(a) == array:  # noqa
        return a[:]
    else:
        assert type(a) == Int64Set  # noqa
        return a.copy()  # type: ignore


def add(a: EfficientSet, val: int) -> EfficientSet:
    if a is None:
        # upgrade None -> int
        return val
    elif type(a) == int:  # noqa
        if a == val:
            return a
        # upgrade int -> array
        return array(ARR_TYPE, [a, val])
    elif type(a) == array:  # noqa
        if val in a:
            return a
        a.append(val)
        if len(a) > ARRAY_SIZE_MAX:
            # upgrade array -> set
            return Int64Set(a)
        return a
    else:
        assert type(a) == Int64Set  # noqa
        a.add(val)  # type: ignore
        return a


def discard(a: EfficientSet, val: int) -> EfficientSet:
    if a is None:
        return None
    elif type(a) == int:  # noqa
        if a == val:
            # downgrade int -> None
            return None
        return a
    elif type(a) == array:  # noqa
        if val not in a:
            return a
        a.remove(val)
        if len(a) == 1:
            # downgrade array -> int
            return a[0]
        else:
            return a
    else:
        assert type(a) == Int64Set  # noqa
        a.discard(val)  # type: ignore
        if len(a) < SET_SIZE_MIN:
            # downgrade set -> array
            b = array(ARR_TYPE)
            b.extend(a)
            return b
        return a


def remove(a: EfficientSet, val: int) -> EfficientSet:
    old_size = size(a)
    a = discard(a, val)
    if size(a) == old_size:
        raise KeyError(val)
    return a


def clear(a: EfficientSet) -> EfficientSet:
    return None


def union(a: EfficientSet, *b: EfficientSet) -> EfficientSet:
    return update(copy(a), *b)


def update(a: EfficientSet, *b: EfficientSet) -> EfficientSet:
    a_temp = to_set(a)
    for b_i in b:
        if b_i is None:
            continue
        elif type(b_i) == int:  # noqa
            a_temp.add(b_i)
        else:
            a_temp.update(b_i)
    return from_set(a_temp)


def intersection(a: EfficientSet, *b: EfficientSet) -> EfficientSet:
    return intersection_update(copy(a), *b)


def intersection_update(a: EfficientSet, *b: EfficientSet) -> EfficientSet:
    a_temp = to_set(a)
    for b_i in b:
        if b_i is None:
            return None
        elif type(b_i) == int:  # noqa
            if b_i not in a_temp:
                return None
            a_temp.clear()
            a_temp.add(b_i)
        else:
            a_temp.intersection_update(b_i)
            if len(a_temp) == 0:
                return None
    return from_set(a_temp)


def difference(a: EfficientSet, *b: EfficientSet) -> EfficientSet:
    return difference_update(copy(a), *b)


def difference_update(a: EfficientSet, *b: EfficientSet) -> EfficientSet:
    a_temp = to_set(a)
    for b_i in b:
        if b_i is None:
            continue
        elif type(b_i) == int:  # noqa
            a_temp.discard(b_i)
        else:
            a_temp.difference_update(b_i)
    return from_set(a_temp)


def symmetric_difference(a: EfficientSet, b: EfficientSet) -> EfficientSet:
    # symmetric difference doesn't make much sense with *args
    return symmetric_difference_update(copy(a), b)


def symmetric_difference_update(a: EfficientSet, b: EfficientSet) -> EfficientSet:
    a_temp = to_set(a)
    if b is None:
        pass
    elif type(b) == int:  # noqa
        if b in a_temp:
            a_temp.discard(b)
        else:
            a_temp.add(b)
    else:
        a_temp.symmetric_difference_update(b)
    return from_set(a_temp)
