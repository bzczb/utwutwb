import typing as T

import attr

_OBJ = T.TypeVar('_OBJ')
_PK = T.TypeVar('_PK')


class Box(T.Generic[_PK, _OBJ]):
    obj: _OBJ
    pk: _PK
    index_mem: tuple

    def __init__(self, obj: _OBJ, pk: _PK): ...
    def __hash__(self): ...
    def __eq__(self, value): ...


@attr.s(slots=True, eq=False)
class DefaultBox(Box[_PK, _OBJ]):
    obj: _OBJ = attr.ib()
    pk: _PK = attr.ib()
    index_mem: tuple = attr.ib(init=False)

    def __hash__(self):
        return object.__hash__(self.pk)

    def __eq__(self, other):
        return self.pk == other.pk
