"""
Contains tests the pdep functions attached to JSONConverter.
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
from src.json_converter import JSONConverter
from Tests import helper_functions

class TestPDEPStreamGenMethods(unittest.TestCase):
    """Test the pdep functions attached to JSONConverter.

    We perform integration testing
    by verifying the behaviour of the create_pdep_stream function,
    the main pdep driver function.
    """
    def test_empty_file(self):
        """Test with empty CSV file."""
        csv_column_names = ["col1", "col2", "col3"]
        field_widths = []
        converter = JSONConverter(field_widths, csv_column_names)
        actual_pdep_marker_stream = \
            helper_functions.create_actual_pdep_marker_stream(converter, field_widths,
                                                              csv_column_names)

        self.assertEqual(actual_pdep_marker_stream.value, 0)

    def test_simple(self):
        """Simple pdep stream generation test.

        [3, 3, 3], ["col1", "col2", "col3"] ->
        00000000111000000000000000000111000000000000000000111000000000000000000000000
        """
        csv_column_names = ["col1", "col2", "col3"]
        field_widths = [3, 3, 3]
        converter = JSONConverter(field_widths, csv_column_names)
        actual_pdep_marker_stream = \
            helper_functions.create_actual_pdep_marker_stream(converter, field_widths,
                                                              csv_column_names)
        #expected_pdep_ms = helper_functions.create_expected_pdep_ms(converter, field_widths, csv_column_names)
        expected_pdep_ms = '00000000111000000000000000000111000000000000000000111000000000000000000000000'

        self.assertEqual(actual_pdep_marker_stream.value, int(expected_pdep_ms, 2))

    def test_simple2(self):
        """Slightly more complex pdep test, more fields and empty field.

        [3, 0, 3, 5, 3], ["col1", "col2", "col3", "col4", "col5"] ->
        000000001110000000000000000001111100000000000000000011100000000000000000000000000000000000 /
        0111000000000000000000000000
        """
        csv_column_names = ["col1", "col2", "col3", "col4", "col5"]
        field_widths = [3, 0, 3, 5, 3]
        converter = JSONConverter(field_widths, csv_column_names)
        actual_pdep_marker_stream = \
            helper_functions.create_actual_pdep_marker_stream(converter, field_widths,
                                                              csv_column_names)
        #expected_pdep_ms = helper_functions.create_expected_pdep_ms(converter, field_widths, csv_column_names)
        expected_pdep_ms = '000000001110000000000000000001111100000000000000000011100000000000' \
                           '0000000000000000000000000111000000000000000000000000'
        self.assertEqual(actual_pdep_marker_stream.value, int(expected_pdep_ms, 2))

    def test_unicode(self):
        """Non-ascii column names.

        Using UTF8. Hard coded JSON boilerplate byte size should remain the same, column name
        boilerplate bytes should expand.

        [3, 3, 3], ["한국어", "中文", "English"] ->
        000000001110000000000000000000001110000000000000000000011100000000000000000000000000000
        Read pdep stream this direction <---. Start of file is RHS, end is LHS.
        """
        csv_column_names = ["한국어", "中文", "English"]
        field_widths = [3, 3, 3]
        converter = JSONConverter(field_widths, csv_column_names)
        actual_pdep_marker_stream = \
            helper_functions.create_actual_pdep_marker_stream(converter, field_widths,
                                                              csv_column_names)
        #expected_pdep_ms = helper_functions.create_expected_pdep_ms(converter, field_widths, csv_column_names)
        expected_pdep_ms = '000000001110000000000000000000001110000000000000000000011100000000000' \
            '000000000000000000'

        self.assertEqual(actual_pdep_marker_stream.value, int(expected_pdep_ms, 2))

if __name__ == '__main__':
    unittest.main()
