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
from src import pdep_stream_gen

class TestPDEPStreamGenMethods(unittest.TestCase):
    """
    Test the functions in pdep_stream_gen_unit_tests.py with PyUnit.
    """

    def test_simple(self):
        """Simple CSV transduction test.

        111.111.111 ->
        ..........111..........111..........111..
        """
        extracted_bits_stream = pablo.BitStream(int('111111111', 2))
        pext_marker_stream = pablo.BitStream(int('100010001000', 2))
        idx_marker_stream = pablo.BitStream(1)
        pack_size = 64
        target_format = TransductionTarget.JSON
        csv_column_names = ["col1", "col2", "col3"]

        self.assertEqual(pdep_stream_gen.main(extracted_bits_stream, pext_marker_stream,
                                              idx_marker_stream, pack_size, target_format,
                                              csv_column_names), 1879277596)

if __name__ == '__main__':
    unittest.main()
