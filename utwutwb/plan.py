import typing as T
from copy import deepcopy

import attr

from utwutwb.condition import And, Condition, Or

if T.TYPE_CHECKING:
    from utwutwb.index import Index


Transformer = T.Callable[['Plan'], 'Plan']


class Unset:
    """Sentinel type"""

    def __repr__(self):
        return 'UNSET'


UNSET = Unset()


class Comparable(T.Protocol):
    def __lt__(self, other: T.Self) -> bool: ...

    def __le__(self, other: T.Self) -> bool: ...

    def __gt__(self, other: T.Self) -> bool: ...

    def __ge__(self, other: T.Self) -> bool: ...

    def __eq__(self, other: object) -> bool: ...

    def __ne__(self, other: object) -> bool: ...


_C = T.TypeVar('_C', bound=Comparable)


@attr.s(auto_attribs=True, slots=True)
class Bound:
    value: Comparable
    inclusive: bool

    def symbol(self) -> str:
        return '<=' if self.inclusive else '<'


OptionalBound = Bound | Unset


class Plan(T.Protocol):
    """Base class for all query plan nodes"""

    def __str__(self) -> str:
        return self.to_s()

    def to_s(self, depth: int = 0) -> str:
        """Render this plan as a string"""

    def transform_inputs(self, transformer: Transformer) -> None:
        """
        Recursively transform any child plans

        Args:
            transformer: function that transforms plan nodes and returns a new node
        Returns:
            nothing - this should happen in-place
        """
        return None

    def transform(self, transformer: Transformer) -> 'Plan':
        """
        Recursively transform this plan using the `transformer` function.

        Args:
            transformer: function that transforms this plan node and returns a new node
        Returns:
            new plan
        """
        self.transform_inputs(transformer)
        return transformer(self)


class Empty(Plan):
    def to_s(self, depth: int = 0) -> str:
        return 'Empty'


@attr.s(auto_attribs=True)
class ScanFilter(Plan):
    """Return all objects in the collection, filtering with `condition`"""

    condition: Condition

    def to_s(self, depth=0):
        return f'ScanFilter: {self.condition}'


@attr.s(auto_attribs=True)
class Filter(Plan):
    """Return all objects return by `input`, filtering with `condition`"""

    condition: Condition
    input: Plan

    def to_s(self, depth=0):
        indent = '  ' * depth
        return f'Filter: {self.condition}\n{indent}  - {self.input.to_s(depth + 1)}'

    def transform_inputs(self, transformer: Transformer) -> None:
        self.input = self.input.transform(transformer)


@attr.s(auto_attribs=True)
class SetOp(Plan):
    """Base class for set operations"""

    inputs: list[Plan]

    def to_s(self, depth=0):
        indent = '  ' * depth
        inputs = '\n'.join([f'{indent}  - {i.to_s(depth + 1)}' for i in self.inputs])
        return f'{self.__class__.__name__}\n{inputs}'

    def transform_inputs(self, transformer: Transformer) -> None:
        self.inputs = [i.transform(transformer) for i in self.inputs]


class Intersect(SetOp):
    """Return the intersection of all `inputs`"""


class Union(SetOp):
    """Return the union of all `inputs`"""


class Difference(SetOp):
    """Return the difference of the first `input` and all other `inputs`"""


@attr.s(frozen=True)
class Range(T.Generic[_C]):
    left: OptionalBound = attr.ib(default=UNSET)
    right: OptionalBound = attr.ib(default=UNSET)

    def combine(self, other: 'Range[_C]') -> 'T.Optional[Range[_C]]':
        left = self._combine_bounds(self.left, other.left, lambda a, b: a > b)
        right = self._combine_bounds(self.right, other.right, lambda a, b: a < b)

        # Check for an invalid range
        if isinstance(left, Bound) and isinstance(right, Bound):
            if left.inclusive and right.inclusive:
                if left.value > right.value:
                    return None
            else:
                if left.value >= right.value:
                    return None

        return Range(
            left=left,
            right=right,
        )

    @staticmethod
    def _combine_bounds(
        a: OptionalBound,
        b: OptionalBound,
        compare: T.Callable[[Comparable, Comparable], bool],
    ) -> OptionalBound:
        if isinstance(a, Unset):
            return b
        elif isinstance(b, Unset):
            return a
        elif a == b:
            return Bound(a.value, a.inclusive and b.inclusive)
        elif compare(a.value, b.value):
            return a
        else:
            return b


@attr.s(auto_attribs=True)
class IndexRange(Plan):
    index: 'Index'
    range: Range

    def to_s(self, depth=0):
        if self.range.left is UNSET:
            assert isinstance(self.range.right, Bound)
            return f'IndexRange: {self.index} {self.range.right.symbol()} {repr(self.range.right.value)}'
        if self.range.right is UNSET:
            assert isinstance(self.range.left, Bound)
            return f'IndexRange: {repr(self.range.left.value)} {self.range.left.symbol()} {self.index}'
        return f'IndexRange: {repr(self.range.left.value)} {self.range.left.symbol()} {self.index} {self.range.right.symbol()} {repr(self.range.right.value)}'

    def __deepcopy__(self, memodict):
        return IndexRange(
            index=self.index,
            range=self.range,
        )


@attr.s(auto_attribs=True)
class IndexLookup(Plan):
    """Return objects by looking up `value` in `index`"""

    index: 'Index'
    value: T.Any

    def to_s(self, depth=0):
        return f'IndexLookup: {self.index} = {repr(self.value)}'

    def __deepcopy__(self, memodict):
        return IndexLookup(index=self.index, value=deepcopy(self.value))


class Planner:
    def plan(self, condition: Condition) -> Plan:
        """
        Convert a syntax tree into a query plan.

        Args:
            condition: logical expression syntax tree
        Returns:
            query plan
        """
        if isinstance(condition, And):
            return Intersect(
                inputs=[self.plan(condition.left), self.plan(condition.right)]
            )
        if isinstance(condition, Or):
            return Union(inputs=[self.plan(condition.left), self.plan(condition.right)])
        return ScanFilter(condition=condition)
