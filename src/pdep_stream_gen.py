"""
Proof of concept for PEXT/PDEP approach to the general transducer problem.

Takes as input a PEXT marker stream and an extracted bit stream (obtained via PEXT
operation on the input file using the PEXT marker stream) and produces a PDEP
marker stream. The PDEP marker stream shows where in an output file the extracted
bits should be inserted in order to complete the desired transduction operation.

For example, consider CSV to JSON transduction. The PEXT MS identifies the location
of bytes in the input CSV file that correspond to the CSV fields that are to be
extracted. The PDEP MS identifies the locations in the output JSON file (relative
to JSON boilerplate bytes) that the CSV fields need to be deposited to complete the
transduction.

All streams are represented as (unbounded) integers. IntWrapper is provided as a
means of passing these integers "by reference". Doing so allows us to make changes
to our "stream" variables inside methods and have these changes persist once the
methods return.
"""
import sys
import os

# workaround to get the import statements below working properly. Required
# if this module can be run as "main"
# see https://stackoverflow.com/questions/16981921/relative-imports-in-python-3
PACKAGE_PARENT = '..'
SCRIPT_DIR = os.path.dirname(os.path.realpath(os.path.join(os.getcwd(),
                                                           os.path.expanduser(__file__))))
sys.path.append(os.path.normpath(os.path.join(SCRIPT_DIR, PACKAGE_PARENT)))

from src.transducer_target_enums import TransductionTarget
from src import pablo
from src.field_width import calculate_field_widths

# Assume we're given the following streams (we need to use Parabix to create them dynamically)
EXTRACTED_BITS_STREAM = pablo.IntWrapper(int('111111', 2))
PEXT_MARKER_STREAM = pablo.IntWrapper(int('00010001000', 2))
IDX_MARKER_STREAM = pablo.IntWrapper(1)

TARGET_FORMAT = TransductionTarget.JSON # this is the only user-provided value?
PACK_SIZE = 64 #user can optionally specify
CSV_COLUMN_NAMES = ["col1", "col2"] # assume we're given as input or can parse

def main():
    """Entry point for the program."""
    pdep_marker_stream = pablo.IntWrapper(0)
    field_widths = calculate_field_widths(PEXT_MARKER_STREAM, IDX_MARKER_STREAM, PACK_SIZE)
    field_type = 0
    for field_width in field_widths:
        field_wrapper = pablo.IntWrapper(extract_field(EXTRACTED_BITS_STREAM, field_width))
        num_boilerplate_bytes = transduce_field(field_wrapper, field_type, TARGET_FORMAT)
        insert_field(field_wrapper, pdep_marker_stream, num_boilerplate_bytes + field_width)
        field_type += 1
        if field_type == len(CSV_COLUMN_NAMES):
            field_type = 0
    print(bin(pdep_marker_stream.value))
    print(pablo.bitstream2stringLE(pdep_marker_stream.value, 30))

def extract_field(extracted_bits_stream_wrapper, field_width):
    """
    Extract field_width number of bits from extracted_bits_stream. Start extraction
    from least signifcant position (i.e. rightmost) position in extracted_bits_stream.
    """
    field_extraction_mask = (1 << field_width) - 1
    extracted_field = field_extraction_mask & extracted_bits_stream_wrapper.value
    extracted_bits_stream_wrapper.value = extracted_bits_stream_wrapper.value >> field_width
    return extracted_field

#TODO handle non ASCII encodings
def transduce_field(field_wrapper, field_type, target):
    """
    Pad extracted field with appropriate boilerplate.
    """
    preceeding_boilerplate_bytes = 0
    following_boilerplate_bytes = 0
    if target == TransductionTarget.JSON:
        # "colname": 
        preceeding_boilerplate_bytes = 4 + len(CSV_COLUMN_NAMES[field_type])
        #,\n TODO quotes around value?
        following_boilerplate_bytes = 2
        #TODO "Please enter column names / parse column names from file"
        if field_type == 0:
            #{\n
            preceeding_boilerplate_bytes += 2
        elif field_type == len(CSV_COLUMN_NAMES) - 1:
            #\n}
            following_boilerplate_bytes += 2
    elif target == TransductionTarget.CSV:
        #TODO
        pass

    field_wrapper.value = field_wrapper.value << following_boilerplate_bytes
    return preceeding_boilerplate_bytes + following_boilerplate_bytes

def insert_field(field_wrapper, pdep_marker_stream, transduced_field_width):
    """AND padded value into PDEP marker stream."""
    pdep_marker_stream.value = (pdep_marker_stream.value << transduced_field_width) \
                               | field_wrapper.value

if __name__ == '__main__':
    main()

