"""
Contains unit tests for the functions in field_width.py
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
from src.pablo import IntWrapper

class TestFieldWidthMethods(unittest.TestCase):
    """
    Test the functions in field_width.py with PyUnit.
    """
    
    def test_bad_input(self):
        """Test with negative stream values"""
        pext_marker_stream = IntWrapper(1)
        idx_marker_stream = IntWrapper(-1)
        pack_size = 64
        self.assertRaises(ValueError, field_width.calculate_field_widths,
                          pext_marker_stream, idx_marker_stream, pack_size)

    def test_bad_input2(self):
        """Test with pack_size that isn't a power of 2"""
        pext_marker_stream = IntWrapper(1)
        idx_marker_stream = IntWrapper(1)
        pack_size = 63
        self.assertRaises(ValueError, field_width.calculate_field_widths,
                          pext_marker_stream, idx_marker_stream, pack_size)

    def test_simple(self):
        """Test with single-pack pext_marker_stream"""
        pext_marker_stream = IntWrapper(int('100010001000', 2))
        idx_marker_stream = IntWrapper(1)
        pack_size = 64
        self.assertEqual(field_width.calculate_field_widths
                         (pext_marker_stream, idx_marker_stream, pack_size), [3, 3, 3])

    def test_simple2(self):
        """Test with single-pack pext_marker_stream. Variable length fields."""
        pext_marker_stream = IntWrapper(int('10000011000', 2))
        idx_marker_stream = IntWrapper(1)
        pack_size = 64
        self.assertEqual(field_width.calculate_field_widths
                         (pext_marker_stream, idx_marker_stream, pack_size), [3, 0, 5])

    def test_multpack(self):
        """Test with multi-pack pext_marker_stream and non-standard pack_size."""
        pext_marker_stream = IntWrapper(int('000000100000000000011000', 2))
        idx_marker_stream = IntWrapper(int('101', 2))
        pack_size = 8
        self.assertEqual(field_width.calculate_field_widths
                         (pext_marker_stream, idx_marker_stream, pack_size), [3, 0, 12])
    def test_multpack2(self):
        """Test with multi-pack pext_marker_stream and standard pack_size."""
        pext_marker_stream = IntWrapper(int('12f051200f31011111', 16))
        idx_marker_stream = IntWrapper(int('11', 2))
        pack_size = 64
        self.assertEqual(field_width.calculate_field_widths
                         (pext_marker_stream, idx_marker_stream, pack_size),
                         [0, 3, 3, 3, 3, 7, 3, 0, 2, 0, 0, 0, 9,
                          2, 3, 1, 5, 0, 0, 0, 1, 2])
    def test_multpack3(self):
        """Test with many-pack pext_marker_stream and non-standard pack_size."""
        pext_marker_stream = IntWrapper(int('1010000011111001100111110000000011011010', 2))
        idx_marker_stream = IntWrapper(int('1011110011', 2))
        pack_size = 4
        self.assertEqual(field_width.calculate_field_widths
                         (pext_marker_stream, idx_marker_stream, pack_size),
                         [1, 1, 0, 1, 0, 8, 0, 0, 0, 0, 2, 0, 2, 0, 0, 0, 0, 5, 1])

    def test_multpack4(self):
        """Test with multi-pack aligned on pack boundaries."""
        pext_marker_stream = IntWrapper(int('100000011000000110000001', 2))
        idx_marker_stream = IntWrapper(int('111', 2))
        pack_size = 8
        self.assertEqual(field_width.calculate_field_widths
                         (pext_marker_stream, idx_marker_stream, pack_size), [0, 6, 0, 6, 0, 6])

if __name__ == '__main__':
    unittest.main()
