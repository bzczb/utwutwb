import typing as T

import attr
from BTrees.Interfaces import IBTree
from BTrees.LOBTree import LOBTree
from BTrees.OOBTree import OOBTree

_OBJ = T.TypeVar('_OBJ')
_PK = T.TypeVar('_PK')


@attr.s(slots=True, eq=False)
class ObjectStorage(T.Generic[_PK, _OBJ]):
    obj: _OBJ = attr.ib()
    pk: _PK = attr.ib()
    index_mem: tuple = attr.ib(init=False)

    def __hash__(self):
        return super().__hash__(self.pk)

    def __eq__(self, other):
        return self.pk == other.pk


class Store(T.Protocol[_PK, _OBJ]):
    def __getitem__(self, pk: _PK) -> ObjectStorage[_PK, _OBJ]:
        found = self.get(pk)
        if found is None:
            raise KeyError(pk)
        return found

    def get(self, pk: _PK) -> T.Optional[ObjectStorage[_PK, _OBJ]]: ...

    def __setitem__(self, pk: _PK, obj: _OBJ) -> None:
        """__setitem__() shouldn't return anything, but we want the ObjectStorage created to be returned"""
        self.set(pk, obj)

    def set(self, pk: _PK, obj: _OBJ) -> ObjectStorage[_PK, _OBJ]: ...

    def __delitem__(self, pk: _PK) -> None:
        self.delete(pk)

    def delete(self, pk: _PK) -> None: ...

    def __contains__(self, pk: _PK) -> bool: ...

    def keys(self) -> T.Iterable[_PK]: ...

    def values(self) -> T.Iterable[ObjectStorage[_PK, _OBJ]]: ...

    def clear(self) -> None: ...


class BTreeStore(Store[_PK, _OBJ]):
    __slots__ = ('_items',)

    def __init__(self, *, integer_primary_key: bool = True):
        self._items: IBTree = LOBTree() if integer_primary_key else OOBTree()

    def get(self, pk: _PK) -> ObjectStorage[_PK, _OBJ]:
        return self._items.get(pk, None)

    def set(self, pk: _PK, obj: _OBJ) -> ObjectStorage[_PK, _OBJ]:
        storage = ObjectStorage(obj=obj, pk=pk)
        self._items[pk] = storage
        return storage

    def delete(self, pk: _PK) -> None:
        del self._items[pk]

    def __contains__(self, pk: _PK) -> bool:
        return pk in self._items

    def keys(self) -> T.Iterable[_PK]:
        return iter(self._items)

    def values(self) -> T.Iterable[ObjectStorage[_PK, _OBJ]]:
        for key in self._items:
            yield self._items[key]

    def clear(self) -> None:
        self._items.clear()


class ListStore(Store[int, _OBJ]):
    __slots__ = ('_items',)

    def __init__(self):
        self._items: list[ObjectStorage[int, _OBJ] | None] = []

    def get(self, pk: int) -> ObjectStorage[int, _OBJ]:
        item = self._items[pk]
        assert item is not None
        return item

    def set(self, pk: int, obj: _OBJ) -> ObjectStorage[int, _OBJ]:
        storage = ObjectStorage(obj=obj, pk=pk)
        if len(self._items) == pk:
            self._items.append(storage)
            return storage
        elif len(self._items) < pk:
            self._items.extend([None] * (pk - len(self._items) + 1))
        assert self._items[pk] is None
        self._items[pk] = storage
        return storage

    def delete(self, pk: int) -> None:
        assert self._items[pk] is not None
        self._items[pk] = None

    def __contains__(self, pk: int) -> bool:
        return (0 <= pk < len(self._items)) and (self._items[pk] is not None)

    def keys(self) -> T.Iterable[int]:
        for i, item in enumerate(self._items):
            if item is not None:
                yield i

    def values(self) -> T.Iterable[ObjectStorage[int, _OBJ]]:
        for item in self._items:
            if item is not None:
                yield item

    def clear(self) -> None:
        for i in range(len(self._items)):
            self._items[i] = None
