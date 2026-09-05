import importlib.util
import pathlib
import unittest
import sys
sys.path.insert(0,str(pathlib.Path(__file__).parents[1] / "benchmarks/clickbench"))
spec = importlib.util.spec_from_file_location('gate', pathlib.Path(__file__).parents[1] / 'benchmarks/clickbench/gate.py')
gate = importlib.util.module_from_spec(spec)
spec.loader.exec_module(gate)

class GateTests(unittest.TestCase):
    def records(self):
        return [{'id': q, 'status': 'success' if q in gate.STAGE3 else 'parse_failure'} for q in range(1, 44)]

    def test_failures_are_not_coverage(self):
        for status in ['parse_failure', 'execution_failure', 'timeout', 'answer_mismatch', 'unverified']:
            rows = self.records()
            rows[0]['status'] = status
            self.assertTrue(gate.validate(rows, gate.STAGE3))
        self.assertTrue(gate.validate(self.records()[:-1], gate.STAGE3))
        invalid_ids = self.records()
        invalid_ids[-1]["id"] = 0
        self.assertTrue(gate.validate(invalid_ids, gate.STAGE3))
        self.assertEqual([], gate.validate(self.records(), gate.STAGE3))

    def test_experiment_subsets_require_explicit_complete_identities(self):
        rows = [{'id': 5, 'status': 'success'}, {'id': 6, 'status': 'success'}]
        self.assertTrue(gate.validate(rows, {5, 6}))
        self.assertEqual([], gate.validate(rows, {5, 6}, expected_ids={5, 6}))
        self.assertTrue(gate.validate(rows[:1], {5, 6}, expected_ids={5, 6}))
        self.assertTrue(gate.validate(rows + rows[:1], {5, 6}, expected_ids={5, 6}))
        self.assertTrue(gate.validate(rows, {5, 6}, expected_ids={5, 6, 44}))
        self.assertTrue(gate.validate([], set(), expected_ids=set()))
        rows[1]['status'] = 'answer_mismatch'
        self.assertTrue(gate.validate(rows, {5, 6}, expected_ids={5, 6}))

    def test_wide_policy_checks_the_value(self):
        rows = self.records()
        rows[3].update(status='answer_mismatch', rows=[['5e18']])
        oracle = {'sum_userid': '10000000000000000000', 'count': 2}
        self.assertEqual([], gate.validate(rows, gate.STAGE3, oracle))
        rows[3].update(status='answer_mismatch', rows=[['-1']])
        self.assertTrue(gate.validate(rows, gate.STAGE3, oracle))

    def test_order_refinement_requires_matching_evidence(self):
        rows = self.records()
        sql = 'SELECT k,count(*) AS c FROM hits GROUP BY k ORDER BY c DESC LIMIT 1;'
        rows[7].update(status='answer_mismatch', sql=sql, rows=[['a','2']])
        evidence = {8: {'original_sql': sql, 'sql': gate.refine_limit(sql,2),
                        'status': 'success', 'rows': [['a',2]],
                        'meta': [{'type':'String'},{'type':'UInt64'}]}}
        self.assertEqual([],gate.validate(rows,gate.STAGE3,None,evidence))
        rows[7]['status']='answer_mismatch'
        evidence[8]['rows']=[['b',2]]
        self.assertTrue(gate.validate(rows,gate.STAGE3,None,evidence))
        evidence[8]['rows']=[['a',2]]
        evidence[8]['sql']=sql
        self.assertTrue(gate.validate(rows,gate.STAGE3,None,evidence))

if __name__ == '__main__':
    unittest.main()
