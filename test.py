import unittest
import hermes

class TestQueue(unittest.TestCase):

    def test_add(self):
        queue = hermes.Queue(['hello world'])
        queue.add('blah')
        self.assertEqual(len(queue), 2)

    def test_pop(self):
        queue = hermes.Queue(['hello world'])
        self.assertEqual(queue.pop(), 'hello world')
        queue.add(['blah blah', 'blah'])
        self.assertFalse(queue.pop(2), ['blah blah', 'blah'])

print(hermes.get_name())
print(hermes.get_name())
print(hermes.get_name())

if __name__ == '__main__':
    unittest.main()
