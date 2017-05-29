"""
Contains unit tests for the functions in field_width.py
"""
import unittest

# workaround to get the import statements below working properly. Required 
# if this module can be run as "main". Adds PythonPrototypes directory to sys path
import sys
import os
PACKAGE_PARENT = '..'
foo = os.path.expanduser(__file__)
bar = os.getcwd()

SCRIPT_DIR = os.path.dirname(os.path.realpath(os.path.join(os.getcwd(),
                                                           os.path.expanduser(__file__))))
sys.path.append(os.path.normpath(os.path.join(SCRIPT_DIR, PACKAGE_PARENT)))

ccc = os.path.join(SCRIPT_DIR, PACKAGE_PARENT)
ccc2 = os.path.normpath(os.path.join(SCRIPT_DIR, PACKAGE_PARENT))

from src import field_width
from src.pablo import IntWrapper

class TestFieldWidthMethods(unittest.TestCase):
    """Test the functions in field_width.py with PyUnit"""

    def test_simple(self):
        """Test with single-pack pext_marker_stream """
        pext_marker_stream = IntWrapper(int('00010001000', 2))
        idx_marker_stream = IntWrapper(1)
        pack_size = 64
        self.assertEqual(field_width.calculate_field_widths
                         (pext_marker_stream, idx_marker_stream, pack_size), [3, 3])

    # def test_isupper(self):
    #     self.assertTrue('FOO'.isupper())
    #     self.assertFalse('Foo'.isupper())

    # def test_split(self):
    #     s = 'hello world'
    #     self.assertEqual(s.split(), ['hello', 'world'])
    #     # check that s.split fails when the separator is not a string
    #     with self.assertRaises(TypeError):
    #         s.split(2)

if __name__ == '__main__':
    unittest.main()
