import typing as T

if T.TYPE_CHECKING:
    from utwutwb.index import Index

_T = T.TypeVar('_T')


class Context(T.Generic[_T]):
    indexes: 'dict[str, list[Index]]'
    objs: set[_T]
    attrs: dict[str, T.Callable[[_T], T.Any]]

    def getattr(self, obj: _T, item: 'str | Index', memory: bool) -> T.Any: ...

    def get_index_memory(self, obj: _T) -> T.Optional[tuple]:
        return None
