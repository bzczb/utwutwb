import typing as T

from BTrees.Interfaces import IBTree
from BTrees.LOBTree import LOBTree
from BTrees.OOBTree import OOBTree

if T.TYPE_CHECKING:
    from utwutwb.box import Box

_OBJ = T.TypeVar('_OBJ')
_PK = T.TypeVar('_PK')


class Store(T.Protocol[_PK, _OBJ]):
    def __getitem__(self, pk: _PK) -> 'Box[_PK, _OBJ]':
        found = self.get(pk)
        if found is None:
            raise KeyError(pk)
        return found

    def get(self, pk: _PK) -> T.Optional['Box[_PK, _OBJ]']: ...

    def __setitem__(self, pk: _PK, obj: 'Box[_PK, _OBJ]') -> None:
        """__setitem__() shouldn't return anything, but we want the ObjectStorage created to be returned"""
        self.set(pk, obj)

    def set(self, pk: _PK, obj: 'Box[_PK, _OBJ]') -> None: ...

    def __delitem__(self, pk: _PK) -> None:
        self.delete(pk)

    def delete(self, pk: _PK) -> None: ...

    def __contains__(self, pk: _PK) -> bool: ...

    def keys(self) -> T.Iterable[_PK]: ...

    def values(self) -> T.Iterable['Box[_PK, _OBJ]']: ...

    def objects(self) -> T.Iterable[_OBJ]:
        for item in self.values():
            yield item.obj

    def clear(self) -> None: ...


class BTreeStore(Store[_PK, _OBJ]):
    __slots__ = ('_items',)

    def __init__(self, *, integer_primary_key: bool = True):
        self._items: IBTree = LOBTree() if integer_primary_key else OOBTree()

    def get(self, pk: _PK) -> 'Box[_PK, _OBJ]':
        return self._items.get(pk, None)

    def set(self, pk: _PK, obj: 'Box[_PK, _OBJ]') -> None:
        self._items[pk] = obj

    def delete(self, pk: _PK) -> None:
        del self._items[pk]

    def __contains__(self, pk: _PK) -> bool:
        return pk in self._items

    def keys(self) -> T.Iterable[_PK]:
        return iter(self._items)

    def values(self) -> T.Iterable['Box[_PK, _OBJ]']:
        for key in self._items:
            yield self._items[key]

    def clear(self) -> None:
        self._items.clear()


class ListStore(Store[int, _OBJ]):
    __slots__ = ('_items',)

    def __init__(self):
        self._items: list['Box[int, _OBJ]' | None] = []

    def get(self, pk: int) -> 'Box[int, _OBJ]':
        item = self._items[pk]
        assert item is not None
        return item

    def set(self, pk: int, obj: 'Box[int, _OBJ]') -> None:
        if len(self._items) == pk:
            self._items.append(obj)
            return
        elif len(self._items) < pk:
            self._items.extend([None] * (pk - len(self._items) + 1))
        assert self._items[pk] is None
        self._items[pk] = obj

    def delete(self, pk: int) -> None:
        assert self._items[pk] is not None
        self._items[pk] = None

    def __contains__(self, pk: int) -> bool:
        return (0 <= pk < len(self._items)) and (self._items[pk] is not None)

    def keys(self) -> T.Iterable[int]:
        for i, item in enumerate(self._items):
            if item is not None:
                yield i

    def values(self) -> T.Iterable['Box[int, _OBJ]']:
        for item in self._items:
            if item is not None:
                yield item

    def clear(self) -> None:
        for i in range(len(self._items)):
            self._items[i] = None
