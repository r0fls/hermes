import unittest
import hermes
import subprocess

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


class TestConsumer(unittest.TestCase):
    subprocess.call(['hermes.py', 'start'])

    def test_create(self):
        c0 = hermes.Consumer()
        self.assertEqual(c0.name, b'test.py/0')
        c1 = hermes.Consumer()
        self.assertEqual(c1.name, b'test.py/1')

if __name__ == '__main__':
    unittest.main()
