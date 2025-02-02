import operator
import struct
import typing as T
from functools import partial

import attr
from BTrees.Interfaces import IBTree
from BTrees.LLBTree import LLBTree
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
from utwutwb.store import ListStore, ObjectStorage, Store

ADDRESS_SIZE = struct.calcsize('P')
assert ADDRESS_SIZE == 8, '64-bit address size required'


_OBJ = T.TypeVar('_OBJ')
_PK = T.TypeVar('_PK')


class WutSortKey:
    # TODO broken
    __slots__ = ('wut', 'obj_sto', 'ordering', 'rowid_desc')

    def __init__(
        self,
        wut: 'Wut',
        ordering: list[tuple[int, bool]],
        rowid_desc: bool,
        id_: int,
    ):
        self.wut = wut
        self.obj_sto: ObjectStorage = wut.store[id_]
        self.ordering = ordering
        self.rowid_desc = rowid_desc

    def __lt__(self, other: T.Self):
        for index, descending in self.ordering:
            sm, om = self.obj_sto.index_mem[index], other.obj_sto.index_mem[index]
            if sm < om:
                return not descending
            if sm > om:
                return descending
        return (self.obj_sto.pk < other.obj_sto.pk) != self.rowid_desc


ComputedAttrs = dict[str, T.Callable[[_OBJ], T.Any]]


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
class Wut(Context[_PK, _OBJ], T.MutableSet):
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

    store: Store = attr.ib()
    id_to_rowid: IBTree = attr.ib()

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
    """all indexes"""
    index_mem_nums: dict[str, int] = attr.ib()
    """indexes that are memorized"""

    _default_obj: T.Any = attr.ib()

    def __init__(
        self,
        objs: T.Iterable[_OBJ] = None,
        *,
        attrs: ComputedAttrs = None,
        indexes: T.Sequence[Index | IndexParams | str] = None,
        parser: Parser | None = None,
        planner: Planner | None = None,
        optimizer: Rule | None = None,
        store: Store | None = None,
        default_obj: T.Any = None,
    ):
        self.indexes = {}
        self.attrs = attrs or {}
        self.parser = parser or Parser()
        self.planner = planner or Planner()
        self.optimizer = optimizer or Chain()
        self.store = store or ListStore()  # BTreeStore()
        self._default_obj = default_obj

        self.count = 0
        self._rowid_counter = 0
        self.id_to_rowid = LLBTree()

        indexes = indexes or []
        for ip in indexes:
            index: Index
            if isinstance(ip, (str, IndexParams)):
                index = RangeIndex(ip)
            else:
                assert isinstance(ip, Index)
                index = ip
            self.indexes.setdefault(index.params.name, []).append(index)

        index_num, index_mem_num = 0, 0
        self.index_nums, self.index_mem_nums = {}, {}
        for index in self._iter_indexes():
            assert index.number is None
            index.number = index_num
            self.index_nums[index.params.name] = index_num
            index_num += 1

            if index.params.memorize:
                index.mem_number = index_mem_num
                self.index_mem_nums[index.params.name] = index_mem_num
                index_mem_num += 1

        if objs is not None:
            self.update(objs)

        self.executors: dict[T.Type[Plan], T.Callable[[Plan], Int64Set]] = {
            ScanFilter: lambda plan: self._execute_filter(
                self.store.keys(),
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
        ) -> T.Callable[[BinOp, ObjectStorage[_PK, _OBJ]], T.Any]:
            def matcher(condition: BinOp, obj: ObjectStorage[_PK, _OBJ]) -> T.Any:
                return op(
                    self.match(condition.left, obj), self.match(condition.right, obj)
                )

            return matcher

        def match_unaryop(
            op: T.Callable[[T.Any], T.Any],
        ) -> T.Callable[[UnaryOp, ObjectStorage[_PK, _OBJ]], T.Any]:
            def matcher(condition: UnaryOp, obj: ObjectStorage[_PK, _OBJ]) -> T.Any:
                return op(self.match(condition.operand, obj))

            return matcher

        self.matchers: dict[
            T.Type[Condition], T.Callable[[Condition, ObjectStorage[_PK, _OBJ]], T.Any]
        ] = {
            Literal: lambda condition, obj: condition.value,  # type: ignore
            Attribute: lambda condition, obj: self.getattr(obj, condition.name, False),  # type: ignore
            cond.Array: lambda condition, obj: {
                self.match(i, obj)
                for i in condition.items  # type: ignore
            },
            **{klass: match_binop(op) for klass, op in self.BINOPS.items()},  # type: ignore
            **{klass: match_unaryop(op) for klass, op in self.UNARY_OPS.items()},  # type: ignore
        }

    def add(self, obj: _OBJ) -> None:
        obj_id = ido.id_from_obj(obj)
        if obj_id in self.id_to_rowid:
            return
        obj_sto = self.store.set(self._rowid_counter, obj)
        im_ls = []
        for index in self._iter_indexes():
            val = index.add(obj_sto, self)
            if index.params.memorize:
                im_ls.append(val)
        obj_sto.index_mem = tuple(im_ls)
        self.id_to_rowid[obj_id] = self._rowid_counter
        self._rowid_counter += 1
        self.count += 1

    def discard(self, obj: _OBJ) -> None:
        obj_id = ido.id_from_obj(obj)
        row_id = self.id_to_rowid.get(obj_id, None)
        if row_id is None:
            return
        obj_sto = self.store.get(obj_id)
        assert obj_sto is not None
        del self.id_to_rowid[obj_id]
        index_mem = iter(obj_sto.index_mem)

        for index in self._iter_indexes():
            if index.params.memorize:
                mem = next(index_mem)
                index.remove(obj_sto, self, mem)
            else:
                index.remove(obj_sto, self)

        self.store.delete(row_id)
        self.count -= 1

    def refresh(self, obj: _OBJ) -> None:
        obj_id = ido.id_from_obj(obj)
        row_id = self.id_to_rowid.get(obj_id, None)
        if row_id not in self.store:
            raise ValueError('item not found')

        obj_sto = self.store[obj_id]
        old_im = iter(obj_sto.index_mem)
        new_im_ls = []

        for index in self._iter_indexes():
            if not index.params.memorize:
                # index not memorized, so it must be constant
                continue
            old_v = next(old_im)
            new_v = index.make_val(obj_sto, self)
            if old_v != new_v:
                index.refresh(obj_sto, self, old_v, new_v)
            new_im_ls.append(new_v)

        obj_sto.index_mem = tuple(new_im_ls)

    def clear(self) -> None:
        self.id_to_rowid.clear()
        self.count = 0
        for index in self._iter_indexes():
            index.clear()
        self.store.clear()

    def clone(self, objs: Int64Set | T.Iterable[_OBJ] | None = None) -> T.Self:
        o_i: T.Iterable[_OBJ]
        if objs is None:
            o_i = []
        elif type(objs) == Int64Set:  # noqa
            o_i = self.objects(objs)
        else:
            o_i = objs

        return type(self)(
            objs=o_i,
            attrs=self.attrs,
            indexes=[index.clone() for index in self._iter_indexes()],
            parser=self.parser,
            planner=self.planner,
            optimizer=self.optimizer,
            default_obj=self._default_obj,
        )

    def update(self, objs: T.Iterable[_OBJ]) -> None:
        for obj in objs:
            self.add(obj)

    def set_default_obj(self, obj: _OBJ) -> None:
        self._default_obj = obj

    def default_obj(self):
        raise self._default_obj

    def getattr(
        self, obj: 'ObjectStorage[_PK, _OBJ]', item: str | Index, memory: bool
    ) -> T.Any:
        if memory:
            assert isinstance(item, Index)
            index = item
            if index.params.memorize:
                assert index.mem_number is not None
                return obj.index_mem[index.mem_number]

        if isinstance(item, Index):
            attr_name = item.params.name
        else:
            attr_name = item

        if attr_name.startswith('`'):
            return self.attrs[attr_name](obj.obj)
        else:
            return getattr(obj.obj, attr_name)

    def get_index_memory(self, obj: _OBJ) -> T.Optional[tuple]:
        obj_id = ido.id_from_obj(obj)
        return self.store[obj_id].index_mem

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

    def match(self, condition: Condition, obj: ObjectStorage[_PK, _OBJ]) -> T.Any:
        matcher = self.matchers.get(condition.__class__)
        if matcher:
            return matcher(condition, obj)

        raise ValueError(f'Unsupported condition: {condition}')

    def __contains__(self, obj: object) -> bool:
        obj_id = ido.id_from_obj(obj)
        return obj_id in self.id_to_rowid

    def __iter__(self) -> T.Iterator[_OBJ]:
        for obj_sto in self.store.values():
            yield obj_sto.obj

    def __len__(self) -> int:
        return self.count

    def sort_ids(self, ids: T.Iterable[int], ordering: list[tuple[str, bool]] = None):
        # TODO broken
        if ordering:
            assert False, 'broken rn'

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

    def objects(self, ids: T.Iterable[int]) -> T.Iterator[_OBJ]:
        for id in ids:
            yield self.store[id].obj

    def sorted_objects(
        self, ids: T.Iterable[int], ordering: list[tuple[str, bool]] = None
    ) -> list[_OBJ]:
        ids_sorted = self.sort_ids(ids, ordering)
        return [self.store[id].obj for id in ids_sorted]

    def list_objects(self, ids: T.Iterable[int]) -> list[_OBJ]:
        return list(self.objects(ids))

    def _iter_indexes(self, *, memorized_only=False) -> T.Iterator[Index]:
        for indexes in self.indexes.values():
            for index in indexes:
                if not memorized_only or index.params.memorize:
                    yield index

    def _execute_filter(self, objs: T.Iterable[int], condition: Condition) -> Int64Set:
        if isinstance(condition, Literal):
            if not condition.value:
                return Int64Set()
            if condition.value:
                if type(objs) == Int64Set:  # noqa
                    return objs
                else:
                    return Int64Set(objs)
            return objs if condition.value else set()
        return Int64Set(filter(lambda o: self.match(condition, self.store[o]), objs))
