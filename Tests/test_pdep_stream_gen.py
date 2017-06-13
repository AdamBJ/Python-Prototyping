"""
Contains tests for the functions in pdep_stream_gen.py.
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
from src.pdep_stream_gen import create_pdep_stream

class TestPDEPStreamGenMethods(unittest.TestCase):
    """Test the functions in pdep_stream_gen_unit_tests.py with PyUnit.

    We perform integration testing for the pdep_stream_gen.py module
    by verifying the behaviour of the create_pdep_stream function,
    which calls the other functions defined in pdep_stream_gen.py
    """
    def test_empty_file(self):
        """Test with empty CSV file."""
        csv_column_names = ["col1", "col2", "col3"]

        pdep_marker_stream = pablo.BitStream(create_pdep_stream([], csv_column_names))
        self.assertEqual(pdep_marker_stream.value, 0)

    def test_simple(self):
        """Simple CSV transduction test.

        [3, 3, 3], ["col1", "col2", "col3"] ->
        00111000000 00111000000 0011100000000
        """
        csv_column_names = ["col1", "col2", "col3"]

        pdep_marker_stream = pablo.BitStream(create_pdep_stream([3, 3, 3], csv_column_names))
        self.assertEqual(pdep_marker_stream.value, int('111000000001110000000011100000000', 2))

    def test_simple2(self):
        """Simple CSV transduction test with empty fields, more complex idx, different pack_size.

        [3, 0, 3, 5, 3], ["col1", "col2", "col3", "col4", "col5"] ->
        00111000000 0011111000000 00111000000 00|000000 0011100000000
        """
        csv_column_names = ["col1", "col2", "col3", "col4", "col5"]

        pdep_marker_stream = pablo.BitStream(create_pdep_stream([3, 0, 3, 5, 3], csv_column_names))
        print(bin(pdep_marker_stream.value))
        self.assertEqual(pdep_marker_stream.value,
                         int('00111000000001111100000000111000000000000000011100000000', 2))

    def test_unicode(self):
        """Non-ascii column names.

        Using UTF8. Hard coded SON boilerplate byte size should remain the same, column name
        boilerplate bytes should expand.

        [3, 3, 3], ["한국어", "中文", "English"] ->
            2 + 7 = 9      2 + 6 = 8    2 + 2 + 9 = 13
        00111000000000 0011100000000 001110000000000000
        Read pdep stream this direction <---. Start of file is RHS, end is LHS.
        """
        csv_column_names = ["한국어", "中文", "English"]

        pdep_marker_stream = pablo.BitStream(create_pdep_stream([3, 3, 3], csv_column_names))
        self.assertEqual(pdep_marker_stream.value,
                         int('1110000000000011100000000001110000000000000', 2))

if __name__ == '__main__':
    unittest.main()
