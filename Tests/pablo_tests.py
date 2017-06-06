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
# differently.
class TestPabloMethods(unittest.TestCase):
    """Contains unit tests for the functions in pablo.py"""
    def test_serial_to_parallel1(self):
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
        csv_bit_streams = pablo.BasisBits()
        pablo.serial_to_parallel(csv_file_as_str, csv_bit_streams)
        self.assertEqual(csv_bit_streams.bit_streams, [0, 0, 7, 7, 0, 0, 3, 5])

if __name__ == '__main__':
    unittest.main()