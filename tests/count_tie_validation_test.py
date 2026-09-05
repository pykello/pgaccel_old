import pathlib
import sys
import unittest
sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1] / 'benchmarks' / 'clickbench'))
from answer_validation import matches_count_ordered_tie

class CountTieValidation(unittest.TestCase):
    def setUp(self):
        self.meta = [{'type': 'String'}, {'type': 'UInt64'}, {'type': 'Float64'}]
        self.reference = [['a', 5, 1.5], ['b', 2, 3.0]]
        self.candidates = [['a', 5, 1.5], ['b', 2, 3.0], ['c', 2, 7.0], ['d', 1, 9.0]]
    def check(self, rows):
        return matches_count_ordered_tie(rows, self.reference, self.candidates, 1, 1, self.meta)
    def test_valid_boundary_alternative(self):
        self.assertTrue(self.check([['a', 5, 1.5], ['c', 2, 7.0]]))
    def test_wrong_aggregate_rejected(self):
        self.assertFalse(self.check([['a', 5, 1.5], ['c', 2, 8.0]]))
    def test_invented_group_rejected(self):
        self.assertFalse(self.check([['a', 5, 1.5], ['invented', 2, 7.0]]))
    def test_rank_and_order_rejected(self):
        self.assertFalse(self.check([['a', 5, 1.5], ['d', 1, 9.0]]))
        self.assertFalse(self.check([['c', 2, 7.0], ['a', 5, 1.5]]))
    def test_string_minimum_is_verified_for_count_ordered_ties(self):
        meta = [{'type': 'String'}, {'type': 'String'}, {'type': 'UInt64'}]
        reference = [['a', 'url-a', 5], ['b', 'url-b', 2]]
        actual = [['a', 'url-a', 5], ['b', 'url-b', 2], ['c', 'url-c', 2]]
        self.assertTrue(matches_count_ordered_tie(
            [['a', 'url-a', 5], ['c', 'url-c', 2]], reference, actual, 1, 2, meta))
        self.assertFalse(matches_count_ordered_tie(
            [['a', 'url-a', 5], ['c', 'invented-minimum', 2]], reference, actual, 1, 2, meta))

    def test_duplicate_group_rejected(self):
        self.assertFalse(matches_count_ordered_tie([['c', 2, 7.0], ['c', 2, 7.0]],
            [['b', 2, 3.0], ['c', 2, 7.0]], self.candidates, 1, 1, self.meta))

if __name__ == '__main__':
    unittest.main()
