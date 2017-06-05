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
from src.pdep_stream_gen import generate_pdep_stream

class TestCSVJSONTransducerMethods(unittest.TestCase):
    """Contains a mix of unit tests and integration/system tests."""
    def test_create_pext_ms(self):
        pass
    def test_create_idx_ms(self):
        pass
    def test_main(self):
        """Integration test for csv_json_transducer.py == system test."""