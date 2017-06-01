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
        pext_marker_stream = pablo.BitStream(int('11101110111', 2))
        idx_marker_stream = pablo.BitStream(1)
        pack_size = 64
        target_format = TransductionTarget.JSON
        csv_column_names = ["col1", "col2", "col3"]

        pdep_marker_stream = pablo.BitStream(pdep_stream_gen.main(pext_marker_stream,
                                                                  idx_marker_stream,
                                                                  pack_size, target_format,
                                                                  csv_column_names))

        self.assertEqual(pablo.get_popcount(pext_marker_stream.value),
                         pablo.get_popcount(pdep_marker_stream.value))
        self.assertEqual(pdep_marker_stream.value, 1879277596)

if __name__ == '__main__':
    unittest.main()
