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
        target_format = TransductionTarget.JSON
        csv_column_names = ["col1", "col2", "col3"]

        pdep_marker_stream = pablo.BitStream(create_pdep_stream([],
                                                                target_format,
                                                                csv_column_names))
        self.assertEqual(pdep_marker_stream.value, 0)

    def test_simple(self):
        """Simple CSV transduction test.

        100010001000 ->
        ..........111..........111..........111..
        """
        target_format = TransductionTarget.JSON
        csv_column_names = ["col1", "col2", "col3"]

        pdep_marker_stream = pablo.BitStream(create_pdep_stream([3, 3, 3],
                                                                target_format,
                                                                csv_column_names))
        self.assertEqual(pdep_marker_stream.value, 1879277596)

    def test_simple2(self):
        """Simple CSV transduction test with empty fields, more complex idx, different pack_size.

        1000110001000001000 ->
        ..........111....................111..........11111..........111..
        """
        target_format = TransductionTarget.JSON
        csv_column_names = ["col1", "col2", "col3", "col4", "col5"]

        pdep_marker_stream = pablo.BitStream(create_pdep_stream([3, 0, 3, 5, 3],
                                                                target_format,
                                                                csv_column_names))
        self.assertEqual(pdep_marker_stream.value, 63050402300395548)

    def test_unicode(self):
        """Non-ascii column names.

        Using UTF8. Hard coded SON boilerplate byte size should remain the same, column name
        boilerplate bytes should expand.

        100010010000000 ->
        2 + 4 + 9=15     2 + 4 + 6=12     2 + 4 + 7 = 13
        000000000000000111000000000000111000000000000011100
        """
        pack_size = 64
        target_format = TransductionTarget.JSON
        csv_column_names = ["한국어", "中文", "English"]

        pdep_marker_stream = pablo.BitStream(create_pdep_stream([3, 3, 3],
                                                                target_format,
                                                                csv_column_names))
        self.assertEqual(pdep_marker_stream.value, 60131377180)

if __name__ == '__main__':
    unittest.main()
