import struct
import typing as T
from functools import partial

import attr
from BTrees.Interfaces import IBTree
from BTrees.LLBTree import LLBTree
from BTrees.LOBTree import LOBTree
from cykhash import Int64Set

import utwutwb.id_ops as ido
import utwutwb.set_ops as so
from utwutwb.context import Context
from utwutwb.index import HashIndex, Index, IndexParams

ADDRESS_SIZE = struct.calcsize('P')
assert ADDRESS_SIZE == 8, '64-bit address size required'


_T = T.TypeVar('_T')


@attr.s(slots=True)
class IndexStorage:
    tree: IBTree = attr.ib()
    none_set: Int64Set = attr.ib()


class WutSortKey:
    __slots__ = ('mem', 'ordering', 'id_', 'rowid', 'rowid_desc')

    def __init__(
        self,
        wut: 'Wut',
        ordering: list[tuple[int, bool]],
        rowid_desc: bool,
        id_: int,
    ):
        self.mem = wut.index_memory[id_]
        self.ordering = ordering
        self.rowid_desc = rowid_desc
        self.id_ = id_
        self.rowid = wut.items_to_rowid[id_]

    def __lt__(self, other: T.Self):
        for index, descending in self.ordering:
            sm, om = self.mem[index], other.mem[index]
            if sm < om:
                return not descending
            if sm > om:
                return descending
        return (self.rowid < other.rowid) != descending


ComputedAttrs = dict[str, T.Callable[[_T], T.Any]]


@attr.s(slots=True, kw_only=True, init=False)
class Wut(Context[_T]):
    all_items: IBTree = attr.ib()
    """
    map of item id to item
    keep items around so they don't get deleted;
    otherwise we cast id directly to item
    """
    index_memory: IBTree = attr.ib()
    """map of item id to remembered index values"""
    items_to_rowid: IBTree = attr.ib()
    """map of item id to row id"""
    rowid_to_items: IBTree = attr.ib()
    """map of row id to item id"""

    count: int = attr.ib()
    """number of items"""
    _rowid_counter: int = attr.ib()
    """
    persistent counter; never decremented
    make sure that regardless of set randomness, the default order out
    is the same as the order in, every time
    important for reproducibility
    """

    attrs: ComputedAttrs = attr.ib()
    indexes: dict[str, list[Index]] = attr.ib()
    index_nums: dict[str, int] = attr.ib()

    _default_obj: T.Any = attr.ib(default=None)

    def __init__(
        self,
        attrs: ComputedAttrs = None,
        indexes: T.Sequence[IndexParams | str] = None,
        objects: T.Iterable[_T] = None,
    ):
        self.attrs = attrs or {}
        indexes = indexes or []

        self.all_items = LOBTree()
        self.index_memory = LOBTree()
        self.tacs_to_items = LOBTree()
        self.items_to_rowid = LLBTree()
        self.rowid_to_items = LLBTree()
        self.count = 0
        self._rowid_counter = 0

        assert len(indexes) > 0, 'at least one index is required'
        index_params = [
            ip if isinstance(ip, IndexParams) else IndexParams(ip) for ip in indexes
        ]
        index_names = {ip.name for ip in index_params}
        if len(index_names) != len(index_params):
            raise ValueError('duplicate index names')
        self.indexes = {ip.name: [HashIndex(ip)] for ip in index_params}
        self.index_nums = {}

        for i, index in enumerate(self._iter_indexes()):
            assert index.number is None
            index.number = i
            self.index_nums[index.params.name] = i

        if objects is not None:
            for obj in objects:
                self.add(obj)

    def add(self, obj: _T) -> None:
        obj_id = ido.id_from_obj(obj)
        if obj_id in self.all_items:
            raise ValueError('item already exists')

        self.all_items[obj_id] = obj

        im_ls = []
        for index in self._iter_indexes():
            val = index.add(obj, self)
            im_ls.append(val)

        self.index_memory[obj_id] = tuple(im_ls)

        self.rowid_to_items[obj_id] = self._rowid_counter
        self.items_to_rowid[self._rowid_counter] = obj_id

        self._rowid_counter += 1
        self.count += 1

    def remove(self, obj: _T) -> None:
        obj_id = ido.id_from_obj(obj)
        if obj_id not in self.all_items:
            raise ValueError('item not found')

        rowid = self.rowid_to_items[obj_id]
        del self.rowid_to_items[rowid]
        del self.items_to_rowid[obj_id]

        for index, mem in zip(self._iter_indexes(), self.index_memory[obj_id]):
            index.remove(obj, self, mem)

        del self.index_memory[obj_id]

        del self.all_items[obj_id]
        self.count -= 1

    def update(self, obj: _T) -> None:
        obj_id = ido.id_from_obj(obj)
        if obj_id not in self.all_items:
            raise ValueError('item not found')

        old_im = self.index_memory[obj_id]
        new_im_ls = []

        for old_v, index in zip(old_im, self._iter_indexes()):
            new_v = index.make_val(obj, self)
            if old_v != new_v:
                index.update(obj, self, old_v, new_v)
            new_im_ls.append(new_v)

        self.index_memory[obj_id] = tuple(new_im_ls)

    def set_default(self, obj: _T) -> None:
        self._default_obj = obj

    def default_obj(self):
        raise NotImplementedError

    def getattr(self, obj: _T, item: str | Index) -> T.Any:
        if isinstance(item, Index):
            obj_id = ido.id_from_obj(obj)
            mem = self.index_memory[obj_id]
            if mem is not None:
                assert item.number is not None
                return mem[item.number]
            attr_name = item.params.name
        else:
            attr_name = item

        if attr_name.startswith('`'):
            return self.attrs[attr_name](obj)
        else:
            return getattr(obj, attr_name)

    def get_index_memory(self, obj: _T) -> T.Optional[tuple]:
        obj_id = ido.id_from_obj(obj)
        return self.index_memory[obj_id]

    def __contains__(self, obj: _T) -> bool:
        obj_id = ido.id_from_obj(obj)
        return obj_id in self.all_items

    def __iter__(self) -> T.Iterator[_T]:
        return iter(self.all_items.values())

    def __len__(self) -> int:
        return self.count

    def sort_ids(self, ids, ordering: list[tuple[str, bool]] = None):
        if ordering is None:
            ordering = []
        order_index_n: list[tuple[int, bool]] = []

        for oi, desc in ordering:
            order_index_n.append((self.index_nums[oi], desc))
        if not ordering:
            rowid_desc = False
        else:
            rowid_desc = ordering[-1][1]

        return sorted(
            ids,
            key=partial(
                WutSortKey,
                self,
                order_index_n,
                rowid_desc,
            ),
        )

    def ids_to_objects(self, ids, ordering: list[tuple[str, bool]] = None):
        ids_sorted = self.sort_ids(ids, ordering)
        return [ido.obj_from_id(id) for id in ids_sorted]

    def _store_index_memory(self, obj, im: tuple):
        self.index_memory[ido.id_from_obj(obj)] = im

    def _del_index_memory(self, obj):
        del self.index_memory[ido.id_from_obj(obj)]

    @staticmethod
    def _add_to_storage(storage: IndexStorage, key, value: int):
        if key is None:
            storage.none_set.add(value)
        else:
            Wut._add_to_tree(storage.tree, key, value)

    @staticmethod
    def _discard_from_storage(storage: IndexStorage, key, value: int) -> None:
        if key is None:
            storage.none_set.discard(value)
        else:
            Wut._discard_from_tree(storage.tree, key, value)

    @staticmethod
    def _add_to_tree(tree: IBTree, key, value: int) -> None:
        tree[key] = so.add(tree.get(key, None), value)

    @staticmethod
    def _discard_from_tree(tree: IBTree, key, value: int) -> None:
        old = tree.get(key, None)
        if old is None:
            return
        new = so.discard(old, value)
        if new is None:
            del tree[key]
        else:
            tree[key] = new

    def _iter_indexes(self) -> T.Iterator[Index]:
        for indexes in self.indexes.values():
            for index in indexes:
                yield index
