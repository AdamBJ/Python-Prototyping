"""
Contains tests for the functions in pablo.py.
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

# Won't add many tests here. Just need it to work for purposes of prototype. Parabix does things
# differently for these methods.
class TestPabloMethods(unittest.TestCase):
    """Contains unit tests for the functions in pablo.py"""

    def test_create_idx_ms(self):
        """Unit test for create_idx_ms."""
        idx_ms = pablo.create_idx_ms(int("11011101111", 2), 64)
        self.assertEqual(idx_ms, 1)

    def test_create_pext_ms(self):
        """Unit test for create_pext_ms."""
        csv_file_as_str = pablo.readfile("Resources/Test/test.csv")
        pext_ms = pablo.create_pext_ms(csv_file_as_str, [",", "\n"], True)
        self.assertEqual(pext_ms, int("11110111011", 2))

    def test_serial_parallel_and_back(self):
        """
        Input: CSV file containing 123\n
        Expected output: [0, 0, 7, 7, 8, 0, 14, 5]

        Explanation:
            123\n = 00110001 ->1
                    00110010 ->2
                    00110011 ->3
                    00001010 ->\n

            Parallel bit streams generated from decomposing:
            Remember that least sig bit of each byte (i.e. right most bit) goes to
            bit stream 0. Also remember that each of the bit streams grow from right to
            left, so the first byte is represented by the rightmost bit of each PBS.

            When reading a PBS, we read it from least sig to most sig, right to left. Least
            sig bit represent first byte of file. Most sig bit represents last byte of file.

            Bit streams are stored in list in ascending order (i.e. leftmost bit stream is bs 0)

            first bit from each byte =  7 = 0000
                                        6 = 0000
                                        5 = 0111
                                        4 = 0111
                                        3 = 1000
                                        2 = 0000
                                        1 = 1110
                                        0 = 0101
        """
        csv_file_as_str = pablo.readfile("Resources/Test/s2p_test.csv")
        csv_bit_streams = [0, 0, 0, 0, 0, 0, 0, 0]
        pablo.serial_to_parallel(csv_file_as_str, csv_bit_streams)
        self.assertEqual(csv_bit_streams, [5, 14, 0, 8, 7, 7, 0, 0])

        reconstituted_bytes = pablo.inverse_transpose(csv_bit_streams, 3)
        self.assertEqual('123', reconstituted_bytes)

    def test_apply_pext(self):
        """
        pext_ms:                11101010111111110
        bit_stream:             10101011101100011
        extracted_bits_stream:  101 1 1 10110001
        """
        pext_ms = int('11101010111111110', 2)
        bit_stream = int('10101011101100011', 2)
        expected_source_bit_stream = int('1011110110001', 2)
        actual_ebs = pablo.apply_pext(bit_stream, pext_ms)
        #print(bin(actual_ebs))
        #print(bin(expected_source_bit_stream))
        self.assertEqual(expected_source_bit_stream, actual_ebs)
    def test_apply_pext_csv_json(self):
        """Integration test for s2p, pext, p2s.

        csv_input:              abcd,ff,12345
        pext_ms:                1111011011111
        extracted_byte_stream:  abcdff12345
        """
        csv_byte_stream = 'abcd,ff,12345'
        pext_ms = int('1111101101111', 2) # bit stream, right to left
        expected_extracted_byte_stream = 'abcdff12345'
        csv_bit_streams = [0, 0, 0, 0, 0, 0, 0, 0]
        pablo.serial_to_parallel(csv_byte_stream, csv_bit_streams)
        extracted_bit_streams = [0, 0, 0, 0, 0, 0, 0, 0]

        for i, stream in enumerate(csv_bit_streams):
            extracted_bit_streams[i] = pablo.apply_pext(stream, pext_ms)

        actual_extracted_byte_stream = pablo.inverse_transpose(extracted_bit_streams,
                                                               len('abcdff12345'))
        self.assertEqual(expected_extracted_byte_stream, actual_extracted_byte_stream)

    def test_apply_pdep(self):
        """Unit test for apply_pdep.

        source_bit_stream:  11010111
        pdep_ms:            11110111001001
        sink_bit_stream:    00000000000000
        result:             11100101001001
        """
        source_bit_stream = int('111010111', 2)
        pdep_ms = int('11110111001001', 2)
        sink_bit_stream = [0]
        expected_result = int('11100101001001', 2)
        pablo.apply_pdep(sink_bit_stream, 0, pdep_ms, source_bit_stream)

        self.assertTrue(sink_bit_stream, expected_result)

if __name__ == '__main__':
    unittest.main()
