"""
Proof of concept for PEXT/PDEP approach to the general transducer problem.

Takes as input a PEXT marker stream and an extracted bit stream (obtained via PEXT
operation on the input file using the PEXT marker stream) and produces a PDEP
marker stream. The PDEP marker stream show wheres in an output file the extracted
bits should be inserted in order to complete the desired transduction operation.

For example, consider CSV to JSON transduction. The PEXT MS identifies the location
of bytes in the input CSV file that correspond to the CSV fields that are to be
extracted. The PDEP MS identifies the location in the output JSON file (relative
to JSON boilerplate bytes) that the CSV fields need to be deposited to complete the
transduction.

All streams are represented as (unbounded) integers. Int_Wrapper is provided as a
means to pass these integers "by reference". Doing so allows us to make changes
to our "stream" variables inside methods and have these changes persist once the
methods return.
"""
import sys
import os

from src import pablo
from src import field_width

# workaround to get the import statements below working properly. Required 
# if this module can be run as "main"
# see https://stackoverflow.com/questions/16981921/relative-imports-in-python-3
PACKAGE_PARENT = '..'
SCRIPT_DIR = os.path.dirname(os.path.realpath(os.path.join(os.getcwd(),
                                                           os.path.expanduser(__file__))))
sys.path.append(os.path.normpath(os.path.join(SCRIPT_DIR, PACKAGE_PARENT)))

# Assume we're given the following streams (we need to use Parabix to create them dynamically)
EXTRACTED_BITS_STREAM = pablo.IntWrapper(int('111111', 2))
PEXT_MARKER_STREAM = pablo.IntWrapper(int('00010001000', 2))
IDX_MARKER_STREAM = pablo.IntWrapper(1)  #TODO add multi-pack testcases
PACK_SIZE = 64

def main():
    """Entry point for the program."""
    PDEP_MARKER_STREAM = pablo.IntWrapper(0)
    field_widths = field_width.calculate_field_widths(PEXT_MARKER_STREAM, IDX_MARKER_STREAM, PACK_SIZE)
    """fieldType = 0
    for field_width in field_widths:
        extracted_field = extract_field(EXTRACTED_BITS_STREAM, field_width)
        transduced_field = transduce_field(extracted_field, fieldType)
        insert_field(transduced_field, PDEP_MARKER_STREAM)
    print(bin(PDEP_MARKER_STREAM))
    """
def extract_field(extracted_bits_stream, field_width):
    """Extract field_width number of bits from extracted_bits_stream."""
    #print(pablo.bitstream2string(EXTRACTED_BITS_STREAM, 10))
    pass

def transduce_field(extracted_field, fieldType):
    """ 
    Pad extracted CSV value with JSON boilerplate.
    
    
    Number of preceeding and following JSON bytes is determined by 
    relative position of the CSV value within final JSON object.
    """
    #print(pablo.bitstream2string(EXTRACTED_BITS_STREAM, 10))
    pass

def insert_field(transduced_field, pdep_marker_stream):
    """AND padded value into PDEP marker stream."""
    pass

main()
