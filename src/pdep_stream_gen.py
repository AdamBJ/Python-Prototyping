""" Contains functions used for pdep marker stream geneartion.

create_pdep_stream:
Main driver function, produces a pdep stream.

transduce_field: Given a field (string of 1 bits) and field type,
pads field with boilerplate bits as appropriate and returns number
of boilerplate bits added.

insert field: insert transduced field into pdep stream
"""
import sys
import os

# workaround to get the import statements below working properly.
# see https://stackoverflow.com/questions/16981921/relative-imports-in-python-3
PACKAGE_PARENT = '..'
SCRIPT_DIR = os.path.dirname(os.path.realpath(os.path.join(os.getcwd(),
                                                           os.path.expanduser(__file__))))
sys.path.append(os.path.normpath(os.path.join(SCRIPT_DIR, PACKAGE_PARENT)))

from src.transducer_target_enums import TransductionTarget
from src import pablo
from src.field_width import calculate_field_widths

#TODO encoding? Supports ASCII and UTF8, not UTF16 (currently 1 byte per JSON BP char)
def create_pdep_stream(field_widths, converter):
    """Generate a bit mask stream for use with the PDEP operation.

    Takes as input a list containing field widths and a target format and produces a PDEP
    marker stream. The PDEP marker stream shows where in an output stream the extracted
    bits should be inserted in order to complete the desired transduction operation.

    All streams are represented as (unbounded) integers. BitStream class is provided as a
    means of passing these integers "by reference". Doing so allows us to make changes
    to our "stream" variables inside methods and have these changes persist once the
    methods return.

    Note that we process each stream from least significant bit to most significant bit
    (i.e. right to left). Scans naturally start from the righthand side of the least sig bit.
     However, as disscussed in the field_width.py doc string, we
    read the input document from left to right. Therefore, while we transduce fields from left
    to right, we process streams (to extract information like field widths) by scanning them from
    right to left.

    Args:
        field_widths: The widths of the fields contained in the input file.
        converter: Object containing attributes and methods used to convert fields
            to a particular format.

    Returns (BitStream):
        The pdep bit stream.

    Examples:
        >>> create_pdep_stream([3, 3, 3], TransductionTarget.JSON,
                                 ["col1", "col2", "col3"])
                                 TODO
    """
    pdep_marker_stream = pablo.BitStream(0)
    field_type = 0
    shift_amnt = 0
    # process fields in the order they appear in the file, i.e. from left to right
    for i, field_width in enumerate(field_widths):
        starts_or_ends_file = True if i == 0 or i == (len(field_widths) - 1) else False
        field_wrapper = pablo.BitStream((1 << field_width) - 1) # create field
        num_boilerplate_bytes_added = converter.transduce_field(field_wrapper, field_type,
                                                                starts_or_ends_file)
        insert_field(field_wrapper, pdep_marker_stream, shift_amnt)
        shift_amnt += num_boilerplate_bytes_added + field_width
        field_type += 1
        if field_type == converter.num_fields_per_unit:
            field_type = 0
        #print(bin(pdep_marker_stream.value)) debug
    #print(bin(pdep_marker_stream.value)) #debug
    return pdep_marker_stream.value

def insert_field(field_wrapper, pdep_marker_stream, shift_amount):
    """OR padded value into PDEP marker stream.

    Stream grows from right to left. First field we process (which is the first
    field that appear in the input file) is the left-most field represented by
    pdep_marker_stream. Last field we process (last field in the file) is the
    rightmost.
    """
    pdep_marker_stream.value = (field_wrapper.value << shift_amount) \
                               | pdep_marker_stream.value
