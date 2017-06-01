"""
Contains unit tests for the functions in pdep_stream_gen.py.
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

from src.transducer_target_enums import TransductionTarget
from src import pablo
from src.pdep_stream_gen import generate_pdep_stream

class TestPDEPStreamGenMethods(unittest.TestCase):
    """Test the functions in pdep_stream_gen_unit_tests.py with PyUnit."""
    def test_empty_file(self):
        """Test with empty CSV file."""
        field_width_stream = pablo.BitStream(0)
        idx_marker_stream = pablo.BitStream(0)
        pack_size = 64
        target_format = TransductionTarget.JSON
        csv_column_names = ["col1", "col2", "col3"]

        pdep_marker_stream = pablo.BitStream(generate_pdep_stream(field_width_stream,
                                                                  idx_marker_stream,
                                                                  pack_size, target_format,
                                                                  csv_column_names))
        self.assertEqual(pdep_marker_stream.value, 0)

    def test_simple(self):
        """Simple CSV transduction test.

        100010001000 ->
        ..........111..........111..........111..
        """
        field_width_stream = pablo.BitStream(int('100010001000', 2))
        idx_marker_stream = pablo.BitStream(1)
        pack_size = 64
        target_format = TransductionTarget.JSON
        csv_column_names = ["col1", "col2", "col3"]

        pdep_marker_stream = pablo.BitStream(generate_pdep_stream(field_width_stream,
                                                                  idx_marker_stream,
                                                                  pack_size, target_format,
                                                                  csv_column_names))
        self.assertEqual(pdep_marker_stream.value, 1879277596)

if __name__ == '__main__':
    unittest.main()
