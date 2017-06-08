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
from src import pdep_stream_gen

# Won't add many tests here. Just need it to work for purposes of prototype. Parabix does things
# differently for these methods.
class TestPabloMethods(unittest.TestCase):
    """Contains unit tests for the functions in pablo.py"""

    def test_serial_to_parallel_and_back(self):
        """
        Input: CSV file containing 123
        Expected output: [0, 0, 7, 7, 0, 0, 3, 5]

        Explanation:
            123 = 00110001 00110010 00110011

            pbs0 = first bit from each byte = 000
            1 = 000
            2 = 111
            3 = 111
            4 = 000
            5 = 000
            6 = 011
            7 = 101
        """
        csv_file_as_str = pablo.readfile("Resources/Test/s2p_test.csv")
        csv_bit_streams = [0, 0, 0, 0, 0, 0, 0, 0]
        pablo.serial_to_parallel(csv_file_as_str, csv_bit_streams)
        self.assertEqual(csv_bit_streams, [0, 0, 7, 7, 0, 0, 3, 5])

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
        expected_extracted_bits_stream = int('1011110110001', 2)
        field_widths = [3, 1, 1, 8]
        actual_ebs = pablo.apply_pext(bit_stream, pext_ms, field_widths)
        #print(bin(actual_ebs))
        #print(bin(expected_extracted_bits_stream))
        self.assertEqual(expected_extracted_bits_stream, actual_ebs)

    def test_apply_pext_csv_json(self):
        """
        csv_input:              abcd,ff,12345
        pext_ms:                1111011011111
        extracted_byte_stream:  abcdff12345
        """
        #self.assertFalse(1)

    def test_apply_pdep(self):
        """
        extracted_bits_stream: 111010111
        pdep_ms:            11110111001001
        targer_bit_stream:  00000000000000
        result:             11100101001001
        """
        #self.assertFalse(1)


if __name__ == '__main__':
    unittest.main()
