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

    def __hash__(self):
        return hash(id(self))


def measure(func, n=100):
    timer = timeit.Timer(func)
    return min(timer.repeat(repeat=n, number=1))


def lookup(objs):
    iset = Wut(objs, indexes=['a'])

    db = sqlite3.connect(':memory:')
    db.execute('CREATE TABLE objs (id INT PRIMARY KEY, a INT)')
    db.execute('CREATE INDEX idx_a ON objs (a)')
    db.executemany(
        'INSERT INTO objs (id, a) VALUES (?, ?)', [(i, x.a) for i, x in enumerate(objs)]
    )

    dex = Dex(objs, ['a'])
    fdex = FrozenDex(objs, ['a'])

    plan = iset.optimize(iset.plan('a = 0'))

    preplanned_result = []
    preplanned_measure = measure(lambda: preplanned_result.append(iset.execute(plan)))

    return {
        'utwutwb IndexedSet': measure(lambda: iset.filter('a = 0')),
        'utwutwb IndexedSet (pre-planned)': preplanned_measure,
        'utwutwb objects out': measure(
            lambda: list(iset.objects(preplanned_result[0]))
        ),
        'ducks Dex': measure(lambda: set(dex[{'a': 0}])),
        'ducks FrozenDex': measure(lambda: set(fdex[{'a': 0}])),
        'sqlite': measure(
            lambda: {
                objs[i]
                for (i,) in db.execute('SELECT id FROM objs WHERE a = 0').fetchall()
            }
        ),
        # "set comprehension": measure(lambda: {x for x in objs if x.a == 0}),
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
results = [lookup([X(a=j % (100_000 // i)) for j in range(100_000)]) for i in n]
graph(n, results, 'Match objects', 'Number of matching objects')


def range_lookup(objs, start, stop):
    iset = Wut(objs, indexes=['a'])

    db = sqlite3.connect(':memory:')
    db.execute('CREATE TABLE objs (id INT PRIMARY KEY, a INT)')
    db.execute('CREATE INDEX idx_a ON objs (a)')
    db.executemany(
        'INSERT INTO objs (id, a) VALUES (?, ?)', [(i, x.a) for i, x in enumerate(objs)]
    )

    dex = Dex(objs, ['a'])

    fdex = FrozenDex(objs, ['a'])

    ducks_q = {'a': {'>=': start, '<': stop}}

    q = f'a >= {start} AND a < {stop}'
    plan = iset.optimize(iset.plan(q))

    preplanned_result = []
    preplanned_measure = measure(lambda: preplanned_result.append(iset.execute(plan)))

    return {
        'utwutwb IndexedSet': measure(lambda: iset.filter(q)),
        'utwutwb IndexedSet (pre-planned)': preplanned_measure,
        'utwutwb objects out': measure(
            lambda: list(iset.objects(preplanned_result[0]))
        ),
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
        'set comprehension': measure(
            lambda: {x for x in objs if x.a >= start and x.a < stop}
        ),
    }


n = [100, 1000, 10000, 50000]
range_results = [range_lookup([X(a=j) for j in range(100_000)], 0, i) for i in n]
graph(n, range_results, 'Match range of objects', 'Number of matching objects')

sys.exit(0)
