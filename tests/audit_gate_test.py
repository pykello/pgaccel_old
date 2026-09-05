import importlib.util
import pathlib
import unittest
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
        self.assertEqual([], gate.validate(self.records(), gate.STAGE3))

    def test_wide_policy_checks_the_value(self):
        rows = self.records()
        rows[3].update(status='answer_mismatch', rows=[['5e18']])
        oracle = {'sum_userid': '10000000000000000000', 'count': 2}
        self.assertEqual([], gate.validate(rows, gate.STAGE3, oracle))
        rows[3].update(status='answer_mismatch', rows=[['-1']])
        self.assertTrue(gate.validate(rows, gate.STAGE3, oracle))

if __name__ == '__main__':
    unittest.main()
