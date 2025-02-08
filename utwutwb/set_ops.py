import typing as T

from utwutwb.box import Box
from utwutwb.constants import ARRAY_SIZE_MAX, SET_SIZE_MIN

EfficientSet: T.TypeAlias = T.Union[set[Box], list[Box], Box, None]


def create(a: T.Iterable[Box] | None = None) -> EfficientSet:
    if a is None:
        return None
    it = iter(a)
    try:
        first = next(it)
    except StopIteration:
        return None

    try:
        second = next(it)
    except StopIteration:
        return first

    ls = [first, second]
    for i in range(ARRAY_SIZE_MAX - 2):
        try:
            ls.append(next(it))
        except StopIteration:
            return ls

    s = set(ls)
    s.update(it)
    return s


def to_set(a: EfficientSet) -> set:
    if a is None:
        return set()
    elif isinstance(a, Box):
        return {a}
    elif type(a) == list:  # noqa
        return set(a)
    else:
        assert type(a) == set  # noqa
        return a


def from_set(a: set) -> EfficientSet:
    if len(a) == 0:
        return None
    elif len(a) == 1:
        return next(iter(a))
    elif len(a) <= ARRAY_SIZE_MAX:
        return list(a)
    else:
        return a


def size(a: EfficientSet) -> int:
    if a is None:
        return 0
    elif type(a) == int:  # noqa
        return 1
    elif type(a) == list:  # noqa
        return len(a)
    else:
        assert type(a) == set  # noqa
        return len(a)


def iterate(a: EfficientSet) -> T.Iterator[Box]:
    if a is None:
        return
    elif isinstance(a, Box):
        yield a
    else:
        yield from a  # type: ignore


def copy(a: EfficientSet) -> EfficientSet:
    if a is None:
        return None
    elif isinstance(a, Box):
        return a
    elif type(a) == list:  # noqa
        return a[:]
    else:
        assert type(a) == set  # noqa
        return a.copy()  # type: ignore


def add(a: EfficientSet, val: Box) -> EfficientSet:
    if a is None:
        # upgrade None -> int
        return val
    elif isinstance(a, Box):
        if a == val:
            return a
        # upgrade int -> array
        return [a, val]
    elif type(a) == list:  # noqa
        if val in a:
            return a
        a.append(val)
        if len(a) > ARRAY_SIZE_MAX:
            # upgrade array -> set
            return set(a)
        return a
    else:
        assert type(a) == set  # noqa
        a.add(val)  # type: ignore
        return a


def discard(a: EfficientSet, val: Box) -> EfficientSet:
    if a is None:
        return None
    elif isinstance(a, Box):
        if a == val:
            # downgrade int -> None
            return None
        return a
    elif type(a) == list:  # noqa
        if val not in a:
            return a
        a.remove(val)
        if len(a) == 1:
            # downgrade array -> int
            return a[0]
        else:
            return a
    else:
        assert type(a) == set  # noqa
        a.discard(val)  # type: ignore
        if len(a) < SET_SIZE_MIN:
            # downgrade set -> array
            b = list(a)
            return b
        return a


def remove(a: EfficientSet, val: Box) -> EfficientSet:
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
        elif isinstance(b_i, Box):
            a_temp.add(b_i)
        else:
            a_temp.update(b_i)  # type: ignore
    return from_set(a_temp)


def intersection(a: EfficientSet, *b: EfficientSet) -> EfficientSet:
    return intersection_update(copy(a), *b)


def intersection_update(a: EfficientSet, *b: EfficientSet) -> EfficientSet:
    a_temp = to_set(a)
    for b_i in b:
        if b_i is None:
            return None
        elif isinstance(b_i, Box):
            if b_i not in a_temp:
                return None
            a_temp.clear()
            a_temp.add(b_i)
        else:
            a_temp.intersection_update(b_i)  # type: ignore
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
        elif isinstance(b_i, Box):
            a_temp.discard(b_i)
        else:
            a_temp.difference_update(b_i)  # type: ignore
    return from_set(a_temp)


def symmetric_difference(a: EfficientSet, b: EfficientSet) -> EfficientSet:
    # symmetric difference doesn't make much sense with *args
    return symmetric_difference_update(copy(a), b)


def symmetric_difference_update(a: EfficientSet, b: EfficientSet) -> EfficientSet:
    a_temp = to_set(a)
    if b is None:
        pass
    elif isinstance(b, Box):
        if b in a_temp:
            a_temp.discard(b)
        else:
            a_temp.add(b)
    else:
        a_temp.symmetric_difference_update(b)  # type: ignore
    return from_set(a_temp)
