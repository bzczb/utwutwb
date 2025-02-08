import os
from copy import deepcopy
from unittest import TestCase

from ruamel.yaml import YAML

from utwutwb.condition import attr, or_
from utwutwb.container import Container
from utwutwb.index import InvertedIndex, RangeIndex
from utwutwb.wut import Wut

FIXTURE_DIR = os.path.join(os.path.dirname(__file__), 'fixtures')

ONLY = ''
OVERWRITE = False


class TestWut(TestCase):
    def test_e2e(self):
        yaml = YAML()
        fixture_path = os.path.join(FIXTURE_DIR, 'e2e.yaml')
        with open(fixture_path) as fp:
            fixture = yaml.load(fp)

        for setup in fixture['setups']:
            objects = {i: Container(o) for i, o in enumerate(setup['objects'])}
            objects_to_id = {v: k for k, v in objects.items()}

            indexes = []

            for yaml_index in setup['indexes']:
                if setup.get('inverted_indexes'):
                    index = InvertedIndex(yaml_index)
                else:
                    index = RangeIndex(yaml_index)
                indexes.append(index)

            iset = Wut(set(objects.values()), indexes=indexes)

            for test in setup['tests']:
                title = test['title']
                if ONLY and ONLY != title:
                    continue
                with self.subTest(title):
                    plan = iset.plan(test['condition'])
                    optimized_plan = iset.optimize(deepcopy(plan))
                    result_ids = iset.execute(deepcopy(optimized_plan))
                    result = set(iset.list_objects(result_ids))

                    if OVERWRITE:
                        test['plan'] = str(plan)
                        test['optimized_plan'] = str(optimized_plan)
                        test['result'] = sorted(objects_to_id[i] for i in result)

                    self.assertEqual(test['plan'].strip(), str(plan).strip())
                    self.assertEqual(
                        test['optimized_plan'].strip(), str(optimized_plan).strip()
                    )
                    self.assertEqual({objects[i] for i in test['result']}, result)

            if OVERWRITE:
                with open(fixture_path, 'w', encoding='utf-8') as fp:
                    yaml.dump(fixture, fp)

    def test_set_abc(self):
        # TODO only works because id of int happens to be the same...
        a = Wut([1, 2, 3])
        b = Wut([2, 3, 4])
        self.assertEqual(Wut([2, 3]), a & b)
        self.assertEqual(Wut([1, 2, 3, 4]), a | b)
        self.assertIn(2, a)
        self.assertNotIn(4, a)

    def test_fluent_interface(self):
        objs = [
            Container({'x': 1, 'y': 1}),
            Container({'x': 2, 'y': 2}),
            Container({'x': 3, 'y': 3}),
        ]
        iset = Wut(objs)

        self.assertEqual(
            {objs[0], objs[1]},
            set(
                iset.list_objects(
                    iset.filter(
                        or_(
                            attr('x').eq(1).and_(attr('y').in_([1, 2])),
                            attr('x').eq(2),
                        )
                    )
                )
            ),
        )
        self.assertEqual(
            {objs[0], objs[1]},
            set(iset.list_objects(iset.filter(attr('x').eq(3).not_()))),
        )
