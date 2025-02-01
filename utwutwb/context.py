import typing as T

import attr

if T.TYPE_CHECKING:
    from utwutwb.index import Index

_T = T.TypeVar('_T')


class Context(T.Generic[_T]):
    indexes: 'dict[str, list[Index]]'
    objs: set[_T]
    attrs: dict[str, T.Callable[[_T], T.Any]]

    def getattr(self, obj: _T, item: str) -> T.Any: ...

    def get_index_memory(self, obj: _T) -> T.Optional[tuple]:
        return None


@attr.s
class SimpleContext(Context[_T]):
    indexes: 'dict[str, list[Index]]' = attr.ib(factory=dict)
    objs: set[_T] = attr.ib(factory=set)
    attrs: dict[str, T.Callable[[_T], T.Any]] = attr.ib(factory=dict)

    def getattr(self, obj: _T, item: str) -> T.Any:
        return getattr(obj, item)
