import typing as T

if T.TYPE_CHECKING:
    from utwutwb.index import Index
    from utwutwb.store import ObjectStorage

_OBJ = T.TypeVar('_OBJ')
_PK = T.TypeVar('_PK')


class Context(T.Generic[_PK, _OBJ]):
    indexes: 'dict[str, list[Index]]'
    objs: set[_OBJ]
    attrs: dict[str, T.Callable[[_OBJ], T.Any]]

    def getattr(
        self, obj: 'ObjectStorage[_PK, _OBJ]', item: 'str | Index', memory: bool
    ) -> T.Any: ...

    def get_index_memory(self, obj: _OBJ) -> T.Optional[tuple]:
        return None
