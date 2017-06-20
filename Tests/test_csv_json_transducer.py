"""
Contains tests for the functions in csv_json_transducer.py.
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
from src import csv_json_transducer
from src import field_width
from src.json_converter import JSONConverter
from Tests import helper_functions

# TODO More *realistic* tests. Visually inspect output JSON, save as "verified" output. Test
# program's output against. Umple did this type of test.


class TestCSVJSONTransducerMethods(unittest.TestCase):
    """Contains a mix of unit tests and integration/system tests."""

    def test_bad_input(self):
        """Test with pack_size that isn't a power of 2"""
        pack_size = 63
        self.assertRaises(
            ValueError, csv_json_transducer.main, pack_size, [""],
            "Resources/Test/malformed_rows.csv")

    def test_bad_input2(self):
        """Test with malformed single line CSV file.

        File is abc,123,haha. No newline marking end of final field.
        """
        pack_size = 64
        self.assertRaises(
            ValueError, csv_json_transducer.main, pack_size, ["hehe", "haha", "hoho"],
            "Resources/Test/malformed_rows.csv")

    def test_bad_input3(self):
        """Test with malformed multi line CSV file.

        Missing final newline character.
        """
        pack_size = 64
        self.assertRaises(
            ValueError, csv_json_transducer.main, pack_size, ["hehe", "haha", "hoho"],
            "Resources/Test/malformed_rows_multi.csv")

    def test_bad_input4(self):
        """Test with malformed multi line CSV file.

        Missing some fields in final row.
        """
        pack_size = 64
        self.assertRaises(
            ValueError, csv_json_transducer.main, pack_size, ["hehe", "haha", "hoho"],
            "Resources/Test/malformed_rows_multi2.csv")


    def test_create_extracted_bit_streams(self):
        """Integration test for pext_ms creation, pext application, s2p, p2s operations.

        Create pext_ms.
        Decompose CSV byte stream, apply pext to each resulting bit stream.
        Recombine extracted bit streams into extracted byte stream.

        Example:
            CSV file: abc,123
            Result: abc123
        """
        csv_file_as_str = pablo.readfile("Resources/Test/test.csv") #12,abc,flap
        fields_pext_ms = pablo.create_pext_ms(csv_file_as_str, [",", "\n"], True)

        csv_bit_streams = [0, 0, 0, 0, 0, 0, 0, 0]
        extracted_bit_streams = [0, 0, 0, 0, 0, 0, 0, 0]
        pablo.serial_to_parallel(csv_file_as_str, csv_bit_streams)
        resassemble_test = pablo.inverse_transpose(csv_bit_streams, 11)
        self.assertEqual(resassemble_test, "12,abc,flap")

        for i, stream in enumerate(csv_bit_streams):
            extracted_bit_streams[i] = pablo.apply_pext(stream, fields_pext_ms)

        extracted_byte_stream = pablo.inverse_transpose(extracted_bit_streams, 9)
        self.assertEqual(extracted_byte_stream, "12abcflap")

    def test_create_bp_bs(self):
        """Input of 123 was resulting in '{\ncol1: ___,\n'."""
        csv_column_names = ["col1"]
        csv_file_as_str = pablo.readfile('Resources/Test/s2p_test.csv')
        pack_size = 64
        field_widths = field_width.calculate_field_widths(csv_file_as_str, pack_size)
        converter = JSONConverter(field_widths, csv_column_names)

        json_bp_byte_stream = converter.create_bpb_stream()
        self.assertEqual('[\n    {\n        "col1": ___\n    }\n]', json_bp_byte_stream)

    def test_first_half(self):
        """Integration test verifying the first half of the transducer.

        Testing from CSV file to pdep marker stream.

        Input: 12,abc,flap
        expected_pdep_ms: 00000000111100000000000000000011100000000000000000011000000000000000000000000
        """
        pack_size = 64
        csv_file_as_str = pablo.readfile("Resources/Test/test.csv")
        field_widths = field_width.calculate_field_widths(csv_file_as_str, pack_size)
        csv_column_names = ["col1", "col2", "col3"]
        converter = JSONConverter(field_widths, csv_column_names)
        actual_pdep_marker_stream = helper_functions.create_actual_pdep_marker_stream(converter,
                                                                                      field_widths,
                                                                                      csv_column_names)
        #expected_pdep_ms = helper_functions.create_expected_pdep_ms(converter, field_widths, csv_column_names)
        expected_pdep_ms = '00000000111100000000000000000011100000000000000000011000000000000000000000000'

        self.assertEqual(actual_pdep_marker_stream.value, int(expected_pdep_ms, 2))

    def test_second_half(self):
        """Integration test verifying the second half of the transducer.

        Testing from PDEP marker stream to JSON file."""
        csv_file_as_str = '12,abc,flap'
        field_widths = [2, 3, 4]
        csv_column_names = ["col1", "col2", "col3"]
        converter = JSONConverter(field_widths, csv_column_names)
        pext_marker_stream = 1979 #11110111011
        pdep_ms = int('00000000111100000000000000000011100000000000000000011000000000000000000000000', 2)

        json_bp_byte_stream = converter.create_bpb_stream()
        json_bp_bit_streams = [0, 0, 0, 0, 0, 0, 0, 0]
        csv_bit_streams = [0, 0, 0, 0, 0, 0, 0, 0]
        pablo.serial_to_parallel(csv_file_as_str, csv_bit_streams)
        pablo.serial_to_parallel(json_bp_byte_stream, json_bp_bit_streams)
        for i in range(8):
            # Extract bits from CSV bit streams and deposit extracted bits in bp bit streams.
            extracted_bits_stream = pablo.apply_pext(csv_bit_streams[i], pext_marker_stream)
            pablo.apply_pdep(json_bp_bit_streams, i, pdep_ms, extracted_bits_stream)

        output_byte_stream = pablo.inverse_transpose(json_bp_bit_streams, len(json_bp_byte_stream))
        expected_output_byte_stream = '[\n    {\n        "col1": 12,\n        "col2": abc,\n        "col3": flap\n    }\n]'
        self.assertEqual(output_byte_stream, expected_output_byte_stream)

    def test_unicode(self):
        """Testing with non-ascii characters in csv file."""
        result = csv_json_transducer.main(64, ["col1"], "Resources/Test/unicode_test.csv")
        self.assertEqual(result, '[\n    {\n        "col1": 한\n    }\n]')

    def test_unicode2(self):
        """Testing with non-ascii characters in large csv file."""
        result = csv_json_transducer.main(64, ["col A", "gul", "chaava", "dabu"], "Resources/Test/unicode_test_large.csv")
        expected_result = pablo.readfile("Resources/Verified_Output/verfied_unicode_test_large.json")
        self.assertEqual(result, expected_result)

    def test_main1(self):
        """Integration test for main() == system test."""
        result = csv_json_transducer.main(64, ["col1"], "Resources/Test/s2p_test.csv")
        self.assertEqual(result, '[\n    {\n        "col1": 123\n    }\n]')

    def test_main2(self):
        """Integration test for main() == system test."""
        result = csv_json_transducer.main(64, ["col A", "col B", "col C"],
                                          "Resources/Test/test.csv")
        self.assertEqual(result,
                         '[\n    {\n        "col A": 12,\n        "col B": abc,\n        "col C": flap\n    }\n]')

    # def test_main3(self):
    #     """Test 250 line CSV file. Takes ~20 minutes on a Ubuntu 16.04 VM with limited resources."""
    #     result = csv_json_transducer.main(64, ["id", "first_name", "last_name", "email",
    #                                            "gender", "ip_address"],
    #                                       "Resources/Test/test_multiline_big.csv")
    #     expected = pablo.readfile("Resources/Verified_Output/test_multiline_big.json")
    #     self.assertEqual(result, expected)

if __name__ == '__main__':
    unittest.main()
