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
from src.transducer_target_enums import TransductionTarget
from src import csv_json_transducer
from src import pablo


class TestFieldWidthMethods(unittest.TestCase):
    """
    Test the functions in field_width.py with PyUnit.

    We perform integration testing for the field_width.py module
    by verifying the behaviour of the calculate_field_widths function,
    which calls the other functions defined in field_width.py
    """

    def test_simple(self):
        """Test with single-pack field_width_stream"""
        field_width_stream = int('11101110111', 2)
        idx_marker_stream = 1
        pack_size = 64
        self.assertEqual(field_width.calculate_field_widths
                         (field_width_stream, idx_marker_stream, pack_size), [3, 3, 3])

    def test_simple2(self):
        """Test with single-pack field_width_stream. Variable length fields."""
        field_width_stream = int('1111100111', 2)
        idx_marker_stream = 1
        pack_size = 64
        self.assertEqual(field_width.calculate_field_widths
                         (field_width_stream, idx_marker_stream, pack_size), [3, 0, 5])

    # def test_empty_fields(self):
    #     """Verify empty fields at line ends are processed correctly.

    #     Earlier version of the transducer didn't calculate field widths
    #     for empty fields at the end of the CSV file properly. This test
    #     makes sure such fields are processed in the way we expect.
    #     """
    #     pack_size = 64
    #     csv_file_as_str = "a,b,,"
    #     pext_marker_stream = pablo.create_pext_ms(csv_file_as_str, [",", "\n"], True)
    #     idx_marker_stream = pablo.create_idx_ms(pext_marker_stream, pack_size)
    #     field_widths = field_width.calculate_field_widths(pext_marker_stream,
    #                                                       idx_marker_stream,
    #                                                       pack_size)

    #     self.assertEqual(field_widths, [1, 1, 0, 0])

    def test_multpack(self):
        """Test with multi-pack field_width_stream and non-standard pack_size."""
        field_width_stream = int('11111111111100111', 2)
        idx_marker_stream = int('101', 2)
        pack_size = 8
        self.assertEqual(field_width.calculate_field_widths
                         (field_width_stream, idx_marker_stream, pack_size), [3, 0, 12])
    def test_multpack2(self):
        """Test with multi-pack field_width_stream and standard pack_size."""
        field_width_stream = int('11010000111110101110110111111111000011001110111111101110111011101110', 2)
        idx_marker_stream = int('11', 2)
        pack_size = 64
        self.assertEqual(field_width.calculate_field_widths
                         (field_width_stream, idx_marker_stream, pack_size),
                         [0, 3, 3, 3, 3, 7, 3, 0, 2, 0, 0, 0, 9, 2, 3, 1, 5, 0, 0, 0, 1, 2])

    def test_multpack3(self):
        """Test with many-pack field_width_stream and non-standard pack_size."""
        field_width_stream = int('101111100000110011000001111111100100101', 2)
        idx_marker_stream = int('1011110011', 2)
        pack_size = 4
        self.assertEqual(field_width.calculate_field_widths
                         (field_width_stream, idx_marker_stream, pack_size),
                         [1, 1, 0, 1, 0, 8, 0, 0, 0, 0, 2, 0, 2, 0, 0, 0, 0, 5, 1])


    def test_multpack4(self):
        """Test with multi-pack aligned on pack boundaries."""
        field_width_stream = int('011111100111111001111110', 2)
        idx_marker_stream = int('111', 2)
        pack_size = 8
        self.assertEqual(field_width.calculate_field_widths
                         (field_width_stream, idx_marker_stream, pack_size), [0, 6, 0, 6, 0, 6])

if __name__ == '__main__':
    unittest.main()
