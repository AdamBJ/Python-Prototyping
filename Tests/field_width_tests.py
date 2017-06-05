"""
Contains tests for the functions in field_width.py
"""
import unittest

# workaround to get the import statements below working properly. Required
# if this module can be run as "main". Adds PythonPrototypes directory to sys path
import sys
import os
PACKAGE_PARENT = '..'
SCRIPT_DIR = os.path.dirname(os.path.realpath(os.path.join(os.getcwd(),
                                                           os.path.expanduser(__file__))))
sys.path.append(os.path.normpath(os.path.join(SCRIPT_DIR, PACKAGE_PARENT)))

from src import field_width
from src.pablo import BitStream

class TestFieldWidthMethods(unittest.TestCase):
    """
    Test the functions in field_width.py with PyUnit.

    We perform integration testing for the field_width.py module
    by verifying the behaviour of the calculate_field_widths function,
    which calls the other functions defined in field_width.py
    """

    def test_simple(self):
        """Test with single-pack field_width_stream"""
        field_width_stream = BitStream(int('100010001000', 2))
        idx_marker_stream = BitStream(1)
        pack_size = 64
        self.assertEqual(field_width.calculate_field_widths
                         (field_width_stream, idx_marker_stream, pack_size), [3, 3, 3])

    def test_simple2(self):
        """Test with single-pack field_width_stream. Variable length fields."""
        field_width_stream = BitStream(int('10000011000', 2))
        idx_marker_stream = BitStream(1)
        pack_size = 64
        self.assertEqual(field_width.calculate_field_widths
                         (field_width_stream, idx_marker_stream, pack_size), [5, 0, 3])

    def test_multpack(self):
        """Test with multi-pack field_width_stream and non-standard pack_size."""
        field_width_stream = BitStream(int('000000100000000000011000', 2))
        idx_marker_stream = BitStream(int('101', 2))
        pack_size = 8
        self.assertEqual(field_width.calculate_field_widths
                         (field_width_stream, idx_marker_stream, pack_size), [12, 0, 3])
    def test_multpack2(self):
        """Test with multi-pack field_width_stream and standard pack_size."""
        field_width_stream = BitStream(int('12f051200f31011111', 16))
        idx_marker_stream = BitStream(int('11', 2))
        pack_size = 64
        self.assertEqual(field_width.calculate_field_widths
                         (field_width_stream, idx_marker_stream, pack_size),
                         [2, 1, 0, 0, 0, 5, 1, 3, 2, 9, 0, 0, 0,
                          2, 0, 3, 7, 3, 3, 3, 3, 0])

    def test_multpack3(self):
        """Test with many-pack field_width_stream and non-standard pack_size."""
        field_width_stream = BitStream(int('1010000011111001100111110000000011011010', 2))
        idx_marker_stream = BitStream(int('1011110011', 2))
        pack_size = 4
        self.assertEqual(field_width.calculate_field_widths
                         (field_width_stream, idx_marker_stream, pack_size),
                         [1, 5, 0, 0, 0, 0, 2, 0, 2, 0, 0, 0, 0, 8, 0, 1, 0, 1, 1])


    def test_multpack4(self):
        """Test with multi-pack aligned on pack boundaries."""
        field_width_stream = BitStream(int('100000011000000110000001', 2))
        idx_marker_stream = BitStream(int('111', 2))
        pack_size = 8
        self.assertEqual(field_width.calculate_field_widths
                         (field_width_stream, idx_marker_stream, pack_size), [6, 0, 6, 0, 6, 0])

if __name__ == '__main__':
    unittest.main()