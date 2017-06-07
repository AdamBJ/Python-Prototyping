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
from src import pdep_stream_gen

# TODO More *realistic* tests. Visually inspect output JSON, save as "verified" output. Test
# program's output against. Umple did this type of test.


class TestCSVJSONTransducerMethods(unittest.TestCase):
    """Contains a mix of unit tests and integration/system tests."""

    def test_bad_input(self):
        """Test with pack_size that isn't a power of 2"""
        pack_size = 63
        self.assertRaises(
            ValueError, csv_json_transducer.main, pack_size, [""], "")

    def test_create_pext_ms(self):
        """Unit test for create_pext_ms."""
        csv_file_as_str = pablo.readfile("Resources/Test/test.csv")
        pext_ms = csv_json_transducer.create_pext_ms(
            csv_file_as_str)
        self.assertEqual(pext_ms, int("11011101111", 2))

    def test_create_extracted_bit_streams(self):
        """Integration test for pext_ms creation, pext application, s2p, p2s operations.

        Create pext_ms.
        Decompose CSV byte stream, apply pext to each resulting bit stream.
        Recombine extracted bit streams into extracted byte stream.

        Example:
            CSV file: abc,123
            Result: abc123
        """
        pack_size = 64
        csv_file_as_str = pablo.readfile("Resources/Test/test.csv") #12,abc,flap
        pext_ms = csv_json_transducer.create_pext_ms(csv_file_as_str)
        idx_ms = csv_json_transducer.create_idx_ms(pext_ms, pack_size)
        field_widths = field_width.calculate_field_widths(pext_ms,
                                                          idx_ms,
                                                          pack_size)
        csv_bit_streams = [0, 0, 0, 0, 0, 0, 0, 0]
        extracted_bit_streams = [0, 0, 0, 0, 0, 0, 0, 0]
        pablo.serial_to_parallel(csv_file_as_str, csv_bit_streams)
        resassemble_test = pablo.inverse_transpose(csv_bit_streams, 11)
        self.assertEqual(resassemble_test, "12,abc,flap")

        for i, stream in enumerate(csv_bit_streams):
            extracted_bit_streams[i] = csv_json_transducer.apply_pext(stream, pext_ms, field_widths)

        extracted_byte_stream = pablo.inverse_transpose(extracted_bit_streams, 9)
        self.assertEqual(extracted_byte_stream, "12abcflap")

    def test_create_idx_ms(self):
        """Unit test for create_idx_ms."""
        idx_ms = csv_json_transducer.create_idx_ms(int("11011101111", 2), 64)
        self.assertEqual(idx_ms, 1)

    def test_first_half(self):
        """Integration test verifying the first half of the transducer."""
        pack_size = 64
        csv_file_as_str = pablo.readfile("Resources/Test/test.csv")
        pext_ms = csv_json_transducer.create_pext_ms(
            csv_file_as_str)
        idx_ms = csv_json_transducer.create_idx_ms(pext_ms, pack_size)
        field_widths = field_width.calculate_field_widths(pext_ms,
                                                          idx_ms,
                                                          pack_size)
        pdep_ms = pdep_stream_gen.create_pdep_stream(field_widths,
                                                     ["col1", "col2", "col3"])

        self.assertEqual(pdep_ms, int("1100000000001110000000000111100", 2))

    def test_create_bp_bs(self):
        """Input of 123 was resulting in '{\ncol1: ___,\n'."""
        csv_column_names = ["col1"]
        csv_file_as_str = pablo.readfile('Resources/Test/s2p_test.csv')
        pext_ms = csv_json_transducer.create_pext_ms(
            csv_file_as_str)
        pack_size = 64
        idx_marker_stream = csv_json_transducer.create_idx_ms(
            pext_ms, pack_size)
        field_widths = field_width.calculate_field_widths(pext_ms, idx_marker_stream, pack_size)
        json_bp_byte_stream = csv_json_transducer.create_bpb_stream(
            field_widths, len(csv_column_names), csv_column_names)
        self.assertEqual('{\ncol1: ___\n}', json_bp_byte_stream)

    def test_second_half(self):
        """Integration test verifying the second half of the transducer."""
        # TODO

    def test_main1(self):
        """Integration test for main() == system test."""
        # TODO  

    def test_main2(self):
        """Integration test for main() == system test

        csv_input:              abcd,ff,12345,,
        extracted_byte_stream:  abcdff12345
        col_names = ["onesy", "two", "flap!", "er"]
        boilerplate_byte: {\nonesy: ____,\ntwo: __,\nflap!: _____,\ner:\n}
        output: {\nonesy: abcd,\ntwo: ff,\nflap!: 12345,\ner:\n}
        """


if __name__ == '__main__':
    unittest.main()
