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
        """Simple CSV transduction test."""
        EXTRACTED_BITS_STREAM = pablo.IntWrapper(int('111111', 2))
        PEXT_MARKER_STREAM = pablo.IntWrapper(int('00010001000', 2))
        IDX_MARKER_STREAM = pablo.IntWrapper(1)
        PACK_SIZE = 64 
        TARGET_FORMAT = TransductionTarget.JSON
        CSV_COLUMN_NAMES = ["col1", "col2"]

        self.assertEqual(pdep_stream_gen.main(EXTRACTED_BITS_STREAM, PEXT_MARKER_STREAM, 
                         IDX_MARKER_STREAM, PACK_SIZE, TARGET_FORMAT, CSV_COLUMN_NAMES), 229404)

if __name__ == '__main__':
    unittest.main()