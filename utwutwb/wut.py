import ctypes
import struct
import typing as T
from functools import partial

import attr
from BTrees.Interfaces import IBTree
from BTrees.LLBTree import LLBTree
from BTrees.LOBTree import LOBTree
from BTrees.OOBTree import OOBTree
from BTrees.QOBTree import QOBTree
from cykhash import Int64Set

import utwutwb.set_ops as so

ADDRESS_SIZE = struct.calcsize('P')
assert ADDRESS_SIZE == 8, '64-bit address size required'

IndexBTrees = {'obj': OOBTree, 'int': LOBTree, 'uint': QOBTree}


_T = T.TypeVar('_T')


@attr.s(slots=True)
class WutIndex:
    name: str = attr.ib()
    key: str | T.Callable = attr.ib(default=None)
    mode: T.Literal['attr', 'dict', 'callable'] = attr.ib(default=None)
    key_type: T.Literal['obj', 'int', 'uint'] = attr.ib(default='obj')

    def __attrs_post_init__(self):
        if self.key is None:
            self.key = self.name
        if self.mode is None:
            if isinstance(self.key, str):
                self.mode = 'attr'
            elif callable(self.key):
                self.mode = 'callable'
            else:
                raise ValueError('mode must be specified for non-str key')

    def get_from_obj(self, obj):
        if self.mode == 'attr':
            return getattr(obj, self.key)
        elif self.mode == 'dict':
            return obj[self.key]
        elif self.mode == 'callable':
            return self.key(obj)


@attr.s(slots=True)
class IndexStorage:
    tree: IBTree = attr.ib()
    none_set: Int64Set = attr.ib()


@attr.s(slots=True)
class IndexMemory:
    values: tuple = attr.ib()
    tacs: tuple = attr.ib()

    def __iter__(self):
        return iter((self.values, self.tacs))


class WutSortKey:
    __slots__ = ('mem', 'ordering', 'id_', 'rowid', 'rowid_desc')

    def __init__(
        self,
        wut: 'Wut',
        ordering: list[tuple[int, bool]],
        rowid_desc: bool,
        id_: int,
    ):
        self.mem = wut._get_index_memory(id_).values
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


@attr.s(slots=True, kw_only=True, init=False)
class Wut(T.Generic[_T]):
    all_items: IBTree = attr.ib()
    """
    map of item id to item
    keep items around so they don't get deleted;
    otherwise we cast id directly to item
    """
    index_memory: IBTree = attr.ib()
    """map of item id to remembered index values"""
    index_storage: dict[str, IndexStorage] = attr.ib()
    """map of index name to index storage"""
    tacs_to_items: IBTree = attr.ib()
    """map of tags and categories to items"""
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

    indices: dict[str, WutIndex] = attr.ib()
    """list of indices"""

    _mutable_objects: bool = attr.ib()
    """whether objects can change from under us"""
    _default_obj: T.Any = attr.ib(default=None)

    @staticmethod
    def id_from_obj(obj):
        # return int(np.uint64(id(obj)).astype(np.int64))
        return id(obj)

    @staticmethod
    def obj_from_id(id_):
        return ctypes.cast(id_, ctypes.py_object).value

    def __init__(
        self,
        indices: T.Sequence[WutIndex | str],
        objects: T.Iterable[_T] = None,
        *,
        mutable_objects=True,
    ):
        self._mutable_objects = mutable_objects

        self.all_items = LOBTree()
        if self._mutable_objects:
            self.index_memory = LOBTree()
        self.index_storage = dict()
        self.tacs_to_items = LOBTree()
        self.items_to_rowid = LLBTree()
        self.rowid_to_items = LLBTree()
        self.count = 0
        self._rowid_counter = 0

        assert len(indices) > 0, 'at least one index is required'
        indices_c = [
            index if isinstance(index, WutIndex) else WutIndex(index)
            for index in indices
        ]
        index_names = {index.name for index in indices_c}
        if len(index_names) != len(indices_c):
            raise ValueError('duplicate index names')
        self.indices = {index.name: index for index in indices_c}

        for index in self.indices.values():
            self.index_storage[index.name] = IndexStorage(
                IndexBTrees[index.key_type](), Int64Set()
            )

        if objects is not None:
            for obj in objects:
                self.add(obj)

    def add(self, obj: _T) -> None:
        obj_id = self.id_from_obj(obj)
        if obj_id in self.all_items:
            raise ValueError('item already exists')
        im, tacs = self._make_index_memory(obj)
        for value, storage in zip(im, self.index_storage.values()):
            self._add_to_storage(storage, value, obj_id)
        for tac in tacs:
            self._add_to_tree(self.tacs_to_items, tac, obj_id)
        self.all_items[obj_id] = obj
        self._store_index_memory(obj)
        self.rowid_to_items[obj_id] = self._rowid_counter
        self.items_to_rowid[self._rowid_counter] = obj_id
        self._rowid_counter += 1
        self.count += 1

    def remove(self, obj: _T) -> None:
        obj_id = self.id_from_obj(obj)
        if obj_id not in self.all_items:
            raise ValueError('item not found')
        im, tacs = self._get_index_memory(obj_id)
        for value, storage in zip(im, self.index_storage.values()):
            self._discard_from_storage(storage, value, obj_id)
        for tac in tacs:
            self._discard_from_tree(self.tacs_to_items, tac, obj_id)
        del self.all_items[obj_id]
        self._del_index_memory(obj)
        rowid = self.rowid_to_items[obj_id]
        del self.items_to_rowid[obj_id]
        del self.rowid_to_items[rowid]
        self.count -= 1

    def update(self, obj: _T) -> None:
        if not self._mutable_objects:
            raise ValueError('objects are not mutable')
        obj_id = self.id_from_obj(obj)
        if obj_id not in self.all_items:
            raise ValueError('item not found')
        im, tacs = self.index_memory[obj_id]
        new_im, new_tacs = self._make_index_memory(obj)
        for v, nv, storage in zip(im, new_im, self.index_storage.values()):
            if im == new_im:
                continue
            self._discard_from_storage(storage, v, obj_id)
            self._add_to_storage(storage, nv, obj_id)
        tacs_set, new_tacs_set = set(tacs), set(new_tacs)
        added_tacs = new_tacs_set - tacs_set
        removed_tacs = tacs_set - new_tacs_set
        for tac in added_tacs:
            self._add_to_tree(self.tacs_to_items, tac, obj_id)
        for tac in removed_tacs:
            self._discard_from_tree(self.tacs_to_items, tac, obj_id)
        self.index_memory[obj_id] = IndexMemory(new_im, new_tacs)

    def set_default(self, obj: _T) -> None:
        self._default_obj = obj

    def default_obj(self):
        raise NotImplementedError

    def __contains__(self, obj: _T) -> bool:
        obj_id = self.id_from_obj(obj)
        return obj_id in self.all_items

    def __iter__(self) -> T.Iterator[_T]:
        return iter(self.all_items.values())

    def __len__(self) -> int:
        return self.count

    def get_indices(self) -> list[str]:
        return list(self.indices)

    def match_count(self, index: str, operator: str, value):
        # TODO
        raise NotImplementedError

    def match(self, index: str, operator: str, value, intersection=None):
        # TODO
        raise NotImplementedError

    def sort_ids(self, ids, ordering: list[tuple[str, bool]] = None):
        if ordering is None:
            ordering = []
        order_index_n: list[tuple[int, bool]] = []
        index_names = list(self.indices)

        for oi, desc in ordering:
            order_index_n.append((index_names.index(oi[0]), desc))
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
        return [self.obj_from_id(id) for id in ids_sorted]

    def _get_index_memory(self, id_) -> IndexMemory:
        if self._mutable_objects:
            return self.index_memory[id_]
        else:
            obj = self.obj_from_id(id_)
            return self._make_index_memory(obj)

    def _store_index_memory(self, obj):
        if self._mutable_objects:
            self.index_memory[self.id_from_obj(obj)] = self._make_index_memory(obj)

    def _del_index_memory(self, obj):
        if self._mutable_objects:
            del self.index_memory[self.id_from_obj(obj)]

    def _make_index_memory(self, obj) -> IndexMemory:
        # TODO litetuple to save more memory??
        return IndexMemory(
            tuple(*[index.get_from_obj(obj) for index in self.indices.values()]),
            obj.get_tac_ids(),
        )

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
