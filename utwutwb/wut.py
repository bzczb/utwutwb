import operator
import struct
import typing as T
from functools import partial

import attr
from BTrees.Interfaces import IBTree
from BTrees.LLBTree import LLBTree
from BTrees.LOBTree import LOBTree
from cykhash import Int64Set
from sqlglot import exp

import utwutwb.id_ops as ido
from utwutwb import condition as cond
from utwutwb.condition import Attribute, BinOp, Condition, Literal, UnaryOp
from utwutwb.context import Context
from utwutwb.index import Index, IndexParams, RangeIndex
from utwutwb.optimize import Chain, Rule
from utwutwb.parse import Parser
from utwutwb.plan import (
    Empty,
    Filter,
    IndexLookup,
    IndexRange,
    Intersect,
    Plan,
    Planner,
    ScanFilter,
    Union,
)

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
        return (self.rowid < other.rowid) != self.rowid_desc


ComputedAttrs = dict[str, T.Callable[[_T], T.Any]]


def int64set_intersection(*sets):
    if not sets:
        return Int64Set()
    if len(sets) == 1:
        return sets[0]
    return sets[0].intersection(*sets[1:])


def int64set_union(*sets):
    if not sets:
        return Int64Set()
    if len(sets) == 1:
        return sets[0]
    return sets[0].union(*sets[1:])


@attr.s(init=False, cmp=False)
class Wut(Context[_T], T.MutableSet):
    BINOPS = {
        cond.Add: operator.add,
        cond.Div: operator.truediv,
        cond.FloorDiv: operator.floordiv,
        cond.BitwiseAnd: operator.and_,
        cond.Xor: operator.xor,
        cond.BitwiseOr: operator.or_,
        cond.Pow: operator.pow,
        cond.Is: operator.is_,
        cond.Lshift: operator.lshift,
        cond.Mod: operator.mod,
        cond.Mul: operator.mul,
        cond.Rshift: operator.rshift,
        cond.Sub: operator.sub,
        cond.Lt: operator.lt,
        cond.Le: operator.le,
        cond.Gt: operator.gt,
        cond.Ge: operator.ge,
        cond.Eq: operator.eq,
        cond.Ne: operator.ne,
        cond.And: lambda l, r: l and r,
        cond.Or: lambda l, r: l or r,
        cond.In: lambda l, r: l in r,
    }
    UNARY_OPS = {
        cond.Not: operator.not_,
        cond.Invert: operator.invert,
    }

    all_items: IBTree = attr.ib()
    """
    map of item id to item
    keep items around so they don't get deleted;
    otherwise we cast id directly to item
    """
    all_item_ids: Int64Set = attr.ib()
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
        objs: T.Iterable[_T] = None,
        *,
        attrs: ComputedAttrs = None,
        indexes: T.Sequence[Index | IndexParams | str] = None,
        parser: Parser | None = None,
        planner: Planner | None = None,
        optimizer: Rule | None = None,
    ):
        self.indexes = {}
        self.attrs = attrs or {}
        self.planner = planner or Planner()
        self.optimizer = optimizer or Chain()
        self.parser = parser or Parser()

        self.all_items = LOBTree()
        self.all_item_ids = Int64Set()
        self.index_memory = LOBTree()
        self.items_to_rowid = LLBTree()
        self.rowid_to_items = LLBTree()
        self.count = 0
        self._rowid_counter = 0

        indexes = indexes or []
        for ip in indexes:
            index: Index
            if isinstance(ip, (str, IndexParams)):
                index = RangeIndex(ip)
            else:
                assert isinstance(ip, Index)
                index = ip
            self.indexes.setdefault(index.params.name, []).append(index)

        self.index_nums = {}
        for i, index in enumerate(self._iter_indexes()):
            assert index.number is None
            index.number = i
            self.index_nums[index.params.name] = i

        if objs is not None:
            self.update(objs)

        self.executors: dict[T.Type[Plan], T.Callable[[Plan], Int64Set]] = {
            ScanFilter: lambda plan: self._execute_filter(
                self.all_item_ids,
                plan.condition,  # type: ignore
            ),
            Filter: lambda plan: self._execute_filter(
                self.execute(plan.input),  # type: ignore
                plan.condition,  # type: ignore
            ),
            Union: lambda plan: int64set_union(
                *(self.execute(i) for i in plan.inputs)  # type: ignore
            ),
            Intersect: lambda plan: int64set_intersection(
                *(self.execute(i) for i in plan.inputs)  # type: ignore
            ),
            IndexLookup: lambda plan: plan.index.lookup(plan.value),  # type: ignore
            IndexRange: lambda plan: plan.index.range(plan.range),  # type: ignore
            Empty: lambda plan: Int64Set(),  # type: ignore
        }

        def match_binop(
            op: T.Callable[[T.Any, T.Any], T.Any],
        ) -> T.Callable[[BinOp, _T], T.Any]:
            def matcher(condition: BinOp, obj: _T) -> T.Any:
                return op(
                    self.match(condition.left, obj), self.match(condition.right, obj)
                )

            return matcher

        def match_unaryop(
            op: T.Callable[[T.Any], T.Any],
        ) -> T.Callable[[UnaryOp, _T], T.Any]:
            def matcher(condition: UnaryOp, obj: _T) -> T.Any:
                return op(self.match(condition.operand, obj))

            return matcher

        self.matchers: dict[T.Type[Condition], T.Callable[[Condition, _T], T.Any]] = {
            Literal: lambda condition, obj: condition.value,  # type: ignore
            Attribute: lambda condition, obj: self.getattr(obj, condition.name),  # type: ignore
            cond.Array: lambda condition, obj: {
                self.match(i, obj)
                for i in condition.items  # type: ignore
            },
            **{klass: match_binop(op) for klass, op in self.BINOPS.items()},  # type: ignore
            **{klass: match_unaryop(op) for klass, op in self.UNARY_OPS.items()},  # type: ignore
        }

    def add(self, obj: _T) -> None:
        obj_id = ido.id_from_obj(obj)
        if obj_id in self.all_items:
            return

        self.all_items[obj_id] = obj
        self.all_item_ids.add(obj_id)

        im_ls = []
        for index in self._iter_indexes():
            val = index.add(obj, self)
            im_ls.append(val)

        self.index_memory[obj_id] = tuple(im_ls)

        self.rowid_to_items[self._rowid_counter] = obj_id
        self.items_to_rowid[obj_id] = self._rowid_counter

        self._rowid_counter += 1
        self.count += 1

    def discard(self, obj: _T) -> None:
        obj_id = ido.id_from_obj(obj)
        if obj_id not in self.all_items:
            return

        self.remove(obj)

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
        self.all_item_ids.discard(obj_id)
        self.count -= 1

    def refresh(self, obj: _T) -> None:
        obj_id = ido.id_from_obj(obj)
        if obj_id not in self.all_items:
            raise ValueError('item not found')

        old_im = self.index_memory[obj_id]
        new_im_ls = []

        for old_v, index in zip(old_im, self._iter_indexes()):
            new_v = index.make_val(obj, self)
            if old_v != new_v:
                index.refresh(obj, self, old_v, new_v)
            new_im_ls.append(new_v)

        self.index_memory[obj_id] = tuple(new_im_ls)

    def update(self, objs: T.Iterable[_T]) -> None:
        for obj in objs:
            self.add(obj)

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

    def filter(self, condition: T.Union[Condition, str, exp.Expression]) -> Int64Set:
        plan = self.plan(condition)
        plan = self.optimize(plan)
        return self.execute(plan)

    def plan(self, condition: T.Union[Condition, str, exp.Expression]) -> Plan:
        if isinstance(condition, (str, exp.Expression)):
            condition = self.parser.parse(condition)
        return self.planner.plan(condition)

    def optimize(self, plan: Plan) -> Plan:
        return self.optimizer(plan, self)

    def execute(self, plan: Plan) -> Int64Set:
        executor = self.executors.get(plan.__class__)
        if executor:
            return executor(plan)
        raise ValueError(f'Unsupported plan: {plan}')

    def match(self, condition: Condition, obj: _T) -> T.Any:
        matcher = self.matchers.get(condition.__class__)
        if matcher:
            return matcher(condition, obj)

        raise ValueError(f'Unsupported condition: {condition}')

    def __contains__(self, obj: object) -> bool:
        obj_id = ido.id_from_obj(obj)
        return obj_id in self.all_items

    def __iter__(self) -> T.Iterator[_T]:
        return iter(self.all_items.values())

    def __len__(self) -> int:
        return self.count

    def sort_ids(self, ids: T.Iterable[int], ordering: list[tuple[str, bool]] = None):
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

    def _iter_indexes(self) -> T.Iterator[Index]:
        for indexes in self.indexes.values():
            for index in indexes:
                yield index

    def _execute_filter(self, objs: Int64Set, condition: Condition) -> Int64Set:
        if isinstance(condition, Literal):
            return objs if condition.value else set()
        return Int64Set(
            filter(lambda o: self.match(condition, ido.obj_from_id(o)), objs)
        )
