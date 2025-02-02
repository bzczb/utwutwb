import platform
import sqlite3
import sys
import timeit
from dataclasses import dataclass

import psutil
from ducks import Dex, FrozenDex
from matplotlib import pyplot as plt

from utwutwb import Wut

print('platform:\t', platform.system())
print('release:\t', platform.release())
print('architecture:\t', platform.machine())
print('processor:\t', platform.processor())
print('RAM:\t\t', f'{round(psutil.virtual_memory().total / (1024.0**3))} GB')
print('Python version:\t', sys.version)


@dataclass(frozen=True)
class X:
    a: int
    b: int

    def __hash__(self):
        return hash(id(self))


def measure(func, n=100):
    timer = timeit.Timer(func)
    return min(timer.repeat(repeat=n, number=1))


def lookup(objs):
    iset = Wut(objs, indexes=['a', 'b'])

    db = sqlite3.connect(':memory:')
    db.execute('CREATE TABLE objs (id INT PRIMARY KEY, a INT, b INT)')
    db.execute('CREATE INDEX idx_a ON objs (a)')
    db.execute('CREATE INDEX idx_b ON objs (b)')
    db.executemany(
        'INSERT INTO objs (id, a, b) VALUES (?, ?, ?)',
        [(i, x.a, x.b) for i, x in enumerate(objs)],
    )

    a_and_b = True  # true -> SQLite becomes the worst by quite a bit
    if a_and_b:
        q = 'a = 0 AND b = 59'
        ducks_q = {'a': 0, 'b': 59}
    else:
        q = 'a = 0'
        ducks_q = {'a': 0}

    dex = Dex(objs, ['a', 'b'])
    fdex = FrozenDex(objs, ['a', 'b'])

    plan = iset.optimize(iset.plan(q))

    preplanned_result = []
    preplanned_measure = measure(lambda: preplanned_result.append(iset.execute(plan)))

    return {
        'utwutwb IndexedSet': measure(lambda: iset.filter(q)),
        'utwutwb IndexedSet (pre-planned)': preplanned_measure,
        'utwutwb objects out': measure(lambda: iset.list_objects(preplanned_result[0])),
        'ducks Dex': measure(lambda: set(dex[ducks_q])),
        'ducks FrozenDex': measure(lambda: set(fdex[ducks_q])),
        'sqlite': measure(
            lambda: {
                objs[i]
                for (i,) in db.execute(f'SELECT id FROM objs WHERE {q}').fetchall()
            }
        ),
        # "set comprehension": measure(lambda: {x for x in objs if x.a == 0 and x.b == 59}),
    }


def graph(x, data, title, xlabel):
    for key in data[0].keys():
        y = [d[key] * 1000 for d in data]
        plt.plot(x, y, label=key)

    plt.xlabel(xlabel)
    plt.ylabel('Time (ms)')
    plt.title(title)
    plt.legend()

    plt.show()


n = [1, 10, 100, 1000]
results = [lookup([X(a=j % (100_000 // i), b=59) for j in range(100_000)]) for i in n]
graph(n, results, 'Match objects', 'Number of matching objects')


def range_lookup(objs, start, stop):
    iset = Wut(objs, indexes=['a', 'b'])

    db = sqlite3.connect(':memory:')
    db.execute('CREATE TABLE objs (id INT PRIMARY KEY, a INT, b INT)')
    db.execute('CREATE INDEX idx_a ON objs (a)')
    db.execute('CREATE INDEX idx_b ON objs (b)')
    db.executemany(
        'INSERT INTO objs (id, a, b) VALUES (?, ?, ?)',
        [(i, x.a, x.b) for i, x in enumerate(objs)],
    )

    dex = Dex(objs, ['a', 'b'])

    fdex = FrozenDex(objs, ['a', 'b'])

    a_and_b = True
    if a_and_b:
        q = f'a >= {start} AND a < {stop} AND b = 59'
        ducks_q = {'a': {'>=': start, '<': stop}, 'b': 59}
        set_comp_q = lambda x: x.a >= start and x.a < stop and x.b == 59  # noqa
    else:
        q = f'a >= {start} AND a < {stop}'
        ducks_q = {'a': {'>=': start, '<': stop}}
        set_comp_q = lambda x: x.a >= start and x.a < stop  # noqa

    plan = iset.optimize(iset.plan(q))

    preplanned_result = []
    preplanned_measure = measure(lambda: preplanned_result.append(iset.execute(plan)))

    return {
        'utwutwb IndexedSet': measure(lambda: iset.filter(q)),
        'utwutwb IndexedSet (pre-planned)': preplanned_measure,
        'utwutwb objects out': measure(lambda: iset.list_objects(preplanned_result[0])),
        'ducks Dex': measure(lambda: set(dex[ducks_q])),
        'ducks FrozenDex': measure(lambda: set(fdex[ducks_q])),
        'sqlite': measure(
            lambda: {
                objs[i]
                for (i,) in db.execute(
                    f'SELECT id FROM objs WHERE a >= {start} AND a < {stop}'
                ).fetchall()
            }
        ),
        'set comprehension': measure(lambda: {x for x in objs if set_comp_q(x)}),
    }


n = [100, 1000, 10000, 50000]
range_results = [range_lookup([X(a=j, b=59) for j in range(100_000)], 0, i) for i in n]
graph(n, range_results, 'Match range of objects', 'Number of matching objects')

sys.exit(0)
