import unittest


class MainTest(unittest.TestCase):
    def test_empty(self):
        self.assertEqual(1, 1)

if __name__ == "__main__":
    unittest.main()
