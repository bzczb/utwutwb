import array
import typing as T

import attr
from BTrees.LOBTree import LOBTree
from BTrees.OOBTree import OOBTree
from BTrees.QOBTree import QOBTree
from cykhash import Int64Set

import utwutwb.id_ops as ido
import utwutwb.set_ops as so
from utwutwb.condition import Array, BinOp, Condition, Eq, Ge, Gt, In, Le, Literal, Lt
from utwutwb.constants import ARR_TYPE
from utwutwb.plan import Bound, IndexLookup, IndexRange, Plan, Range, Union, Unset

if T.TYPE_CHECKING:
    from utwutwb.context import Context

_T = T.TypeVar('_T')
IndexBTrees = {'obj': OOBTree, 'int': LOBTree, 'uint': QOBTree}


@attr.s(slots=True)
class IndexParams:
    name: str = attr.ib()
    key_type: T.Literal['obj', 'int', 'uint'] = attr.ib(default='obj')

    mode: T.Literal['direct', 'computed'] = attr.ib(init=False)

    def __attrs_post_init__(self):
        if self.name.startswith('`'):
            self.mode = 'computed'
        else:
            self.mode = 'direct'

    def make_btree(self):
        return IndexBTrees[self.key_type]()


@T.runtime_checkable
class Index(T.Protocol[_T]):
    params: IndexParams
    number: int | None

    def add(self, obj: _T, ctx: 'Context[_T]', val: T.Any = None) -> T.Any:
        """
        Add `obj` to the index
        Returns a storable version of the vals.
        """

    def remove(self, obj: _T, ctx: 'Context[_T]', val: T.Any = None) -> None:
        """Remove `obj` from the index"""

    def refresh(
        self, obj: _T, ctx: 'Context[_T]', old_val: T.Any, new_val: T.Any = None
    ) -> None:
        """Update `obj` in the index"""

    def clear(self) -> None:
        """Remove all objects from the index"""

    def match(self, condition: 'BinOp', operand: 'Condition') -> 'T.Optional[Plan]':
        """
        Determine if this index can serve the given `condition`.

        This assumes the optimizer has already found which side of the condition is the attribute.

        Args:
            condition: the entire binary operator
            operand: the side of the binary operator opposite the attribute
        Returns:
            `None` if this index can't serve the condition.
            `IndexLoop` plan if it can.
        """

    def make_val(self, obj: _T, ctx: 'Context[_T]') -> T.Any:
        """
        Make a storable version of the attribute value for the given object
        """

    def _load_val(self, val: T.Any) -> list: ...

    def _store_val(self, val: list) -> T.Any: ...


@T.runtime_checkable
class SupportsLookup(T.Protocol):
    def lookup(self, value: T.Any) -> Int64Set:
        """
        Get members from the index.

        Args:
            value: Attribute value to lookup
        Returns:
            Result ID set
        """


@T.runtime_checkable
class SupportsRange(T.Protocol):
    def range(self, range: 'Range') -> Int64Set:
        """
        Get members from the index base on a range of values.

        Args:
            range: Range
        Returns:
            Result ID set
        """


class HashIndex(SupportsLookup, Index[_T]):
    """
    Hash table index.

    This maps object attribute values to sets of objects.

    This can match equality expressions, e.g. `a = 1`

    Args:
        attr: name of the attribute to index
    """

    tree: T.Any  # really btree
    none_set: Int64Set
    no_none_allowed: T.ClassVar[bool] = False

    def __init__(self, attr: str | IndexParams):
        self.params = attr if isinstance(attr, IndexParams) else IndexParams(attr)
        self.tree = self.params.make_btree()
        if not self.no_none_allowed:
            self.none_set = Int64Set()
        self.number = None

    def add(self, obj: _T, ctx: 'Context[_T]', val: T.Any = None) -> list:
        obj_id = ido.id_from_obj(obj)
        ret_vals = []
        if val is None:
            val = self._extract_val(obj, ctx, False)
        else:
            val = self._load_val(val)
        for v in val:
            if v is None:
                self.none_set.add(obj_id)
            else:
                dest_set = self.tree.get(v, None)
                dest_set2 = so.add(dest_set, obj_id)
                if dest_set is not dest_set2:
                    self.tree[v] = dest_set2
            ret_vals.append(v)
        return self._store_val(ret_vals)

    def remove(self, obj: _T, ctx: 'Context[_T]', val: T.Any = None) -> None:
        obj_id = ido.id_from_obj(obj)
        if val is None:
            val = self._extract_val(obj, ctx, True)
        else:
            val = self._load_val(val)
        for v in val:
            if v is None:
                self.none_set.discard(obj_id)
            else:
                dest_set = self.tree.get(v, None)
                if dest_set is None:
                    continue
                dest_set2 = so.discard(dest_set, obj_id)
                if dest_set2 is None:
                    del self.tree[v]
                elif dest_set is not dest_set2:
                    self.tree[v] = dest_set2

    def refresh(
        self, obj: _T, ctx: 'Context[_T]', old_val: T.Any, new_val: T.Any = None
    ) -> None:
        obj_id = ido.id_from_obj(obj)
        old_val = self._load_val(old_val)
        ret_vals = []
        if new_val is None:
            new_val = [*self._extract_val(obj, ctx, False)]
        else:
            new_val = self._load_val(new_val)

        old_val_s, new_val_s = set(old_val), set(new_val)
        added_s = new_val_s - old_val_s
        removed_s = old_val_s - new_val_s

        for v in removed_s:
            if v is None:
                self.none_set.discard(obj_id)
                continue
            dest_set = self.tree.get(v, None)
            if dest_set is None:
                continue
            dest_set2 = so.discard(dest_set, obj_id)
            if dest_set2 is None:
                del self.tree[v]
            elif dest_set is not dest_set2:
                self.tree[v] = dest_set2

        for v in added_s:
            if v is None:
                self.none_set.add(obj_id)
            else:
                dest_set = self.tree.get(v, None)
                dest_set2 = so.add(dest_set, obj_id)
                if dest_set is not dest_set2:
                    self.tree[v] = dest_set2

        return self._store_val(new_val)

    def clear(self) -> None:
        self.tree.clear()
        self.none_set.clear()

    def make_val(self, obj: T.Any, ctx) -> None:
        return self._store_val(list(self._extract_val(obj, ctx, False)))

    def lookup(self, val: T.Any) -> Int64Set:
        if val is None:
            obj_ids = self.none_set
        else:
            obj_ids = so.iterate(self.tree.get(val, None))
        return Int64Set(obj_ids)

    def match(self, condition: 'BinOp', operand: 'Condition') -> 'T.Optional[Plan]':
        if isinstance(condition, Eq) and isinstance(operand, Literal):
            return IndexLookup(index=self, value=operand.value)
        if (
            isinstance(condition, In)
            and operand is condition.right
            and isinstance(operand, Array)
            and all(isinstance(i, Literal) for i in operand.items)
        ):
            return Union(
                inputs=[
                    IndexLookup(index=self, value=T.cast(Literal, i).value)
                    for i in operand.items
                ]
            )
        return None

    def _extract_val(
        self, obj: _T, ctx: 'Context[_T]', memory: bool
    ) -> T.Iterable[T.Any]:
        yield ctx.getattr(obj, self, memory)

    def _load_val(self, value: T.Any) -> list:
        return [value]

    def _store_val(self, value: list) -> T.Any:
        assert len(value) == 1
        return value[0]

    def __str__(self) -> str:
        return f'{self.__class__.__name__}({self.params.name})'


class RangeIndex(SupportsRange, HashIndex[_T]):
    INVERSE_COMPARISONS: dict[T.Type[BinOp], T.Type[BinOp]] = {
        Lt: Gt,
        Gt: Lt,
        Le: Ge,
        Ge: Le,
    }
    COMPARISONS = tuple(INVERSE_COMPARISONS)
    COMPARISON_RANGES: dict[T.Type[BinOp], T.Callable[[T.Any], Range]] = {
        Lt: lambda val: Range(right=Bound(val, False)),
        Gt: lambda val: Range(left=Bound(val, False)),
        Le: lambda val: Range(right=Bound(val, True)),
        Ge: lambda val: Range(left=Bound(val, True)),
    }

    def range(self, range: Range[T.Any]) -> Int64Set:
        if type(range.left) == Unset:  # noqa
            left = None
            excludemin = False
        else:
            rleft = T.cast(Bound, range.left)
            left = rleft.value
            excludemin = not rleft.inclusive
        if type(range.right) == Unset:  # noqa
            right = None
            excludemax = False
        else:
            rright = T.cast(Bound, range.right)
            right = rright.value
            excludemax = not rright.inclusive

        vals = self.tree.values(
            left, right, excludemin=excludemin, excludemax=excludemax
        )
        result_set = Int64Set()
        for val in vals:
            result_set.update(so.iterate(val))
        return result_set

    def match(self, condition: BinOp, operand: Condition) -> T.Optional[Plan]:
        if isinstance(condition, self.COMPARISONS) and isinstance(operand, Literal):
            comparison: T.Type[BinOp] = type(condition)
            if operand is condition.left:
                comparison = self.INVERSE_COMPARISONS.get(comparison, comparison)
            return IndexRange(
                index=self, range=self.COMPARISON_RANGES[comparison](operand.value)
            )
        return super().match(condition, operand)


class InvertedIndex(HashIndex[_T]):
    """
    Same as a `HashIndex`, except this assumes the attribute is a collection of values.

    This matches IN expressions, e.g. `1 in a`
    """

    no_none_allowed = True

    def _extract_val(
        self, obj: _T, ctx: 'Context[_T]', memory: bool
    ) -> T.Iterable[T.Any]:
        for val in ctx.getattr(obj, self.params.name, memory):
            # TODO throw if None
            yield val

    def match(self, condition: BinOp, operand: Condition) -> T.Optional[Plan]:
        if (
            isinstance(condition, In)
            and operand is condition.left
            and isinstance(operand, Literal)
        ):
            return IndexLookup(index=self, value=operand.value)
        return None

    def _load_val(self, value):
        return value

    def _store_val(self, value):
        return list(value)


class InvertedArrayIndex(InvertedIndex[_T]):
    def _store_val(self, value: list) -> array.array:
        return array.array(ARR_TYPE, value)
