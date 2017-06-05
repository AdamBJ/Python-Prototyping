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
def create_pdep_stream(field_widths, target_format, csv_column_names):
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
        target_format: E.g. JSON, CSV, ...
        csv_column_names: Names of the columns in the input CSV file (if target_format == JSON)

    Returns (BitStream):
        The pdep bit stream.

    Examples:
        >>> create_pdep_stream([3, 3, 3], TransductionTarget.JSON,
                                 ["col1", "col2", "col3"])
        1879277596

        1879277596 when viewed as a bit stream is ..........111..........111..........111..
    """
    pdep_marker_stream = pablo.BitStream(0)
    field_type = 0
    # process fields in the order they appear in the file, i.e. from left to right
    for field_width in field_widths:
        field_wrapper = pablo.BitStream((1 << field_width) - 1) # create field
        num_boilerplate_bytes_added = transduce_field(field_wrapper, field_type, target_format,
                                                      csv_column_names)
        insert_field(field_wrapper, pdep_marker_stream, num_boilerplate_bytes_added + field_width)
        field_type += 1
        if field_type == len(csv_column_names):
            field_type = 0
        #print(bin(pdep_marker_stream.value)) debug
    #print(bin(pdep_marker_stream.value)) #debug
    return pdep_marker_stream.value

# TODO check handles non-ASCII encodings (everything but UTF-16 should work)
# TODO remove csv_column_names as argument. Only needed if target is CSV. Parse/prompt
# user if required
def transduce_field(field_wrapper, field_type, target, csv_column_names):
    """ Pad extracted field with appropriate boilerplate.

    Args:
        field_wrapper (BitStream): The field to trasduce.
        field_type: A scalar describing the type of the field.
        csv_column_names: The names of the columns in the input CSV file.
     Returns:
         Number of boilerplate padding bytes added.
     Example:
        >>>transduce_field(111, 0, JSON, ["col1","col2"])
        10
        Note: original field transformed from 111 to (00000000)11100
    """
    preceeding_boilerplate_bytes = 0
    following_boilerplate_bytes = 0
    if target == TransductionTarget.JSON:
        #TODO "Please enter column names / parse column names from file"
        # "colname": 
        preceeding_boilerplate_bytes = 4 + len(csv_column_names[field_type].encode('utf-8'))
        #,\n  or \n} TODO quotes around value?
        following_boilerplate_bytes = 2
        if field_type == 0:
            #{\n
            preceeding_boilerplate_bytes += 2
    elif target == TransductionTarget.CSV:
        #TODO
        pass

    field_wrapper.value = field_wrapper.value << following_boilerplate_bytes
    return preceeding_boilerplate_bytes + following_boilerplate_bytes

def insert_field(field_wrapper, pdep_marker_stream, shift_amount):
    """OR padded value into PDEP marker stream. 
    
    Stream grows from right to left. First field we process (which is the first
    field that appear in the input file) is the left-most field represented by 
    pdep_marker_stream. Last field we process (last field in the file) is the
    rightmost.
    """
    pdep_marker_stream.value = (pdep_marker_stream.value << shift_amount) \
                               | field_wrapper.value

if __name__ == '__main__':
    # Assume we're given the following streams (we need to use Parabix to create them dynamically)
    FIELD_WIDTH_STREAM_WRAPPER = pablo.BitStream(int("100010001000", 2))
    IDX_MARKER_STREAM_WRAPPER = pablo.BitStream(1)
    PACK_SIZE = 64 #user can optionally specify
    TARGET_FORMAT = TransductionTarget.JSON # this is the only user-provided value?
    CSV_COLUMN_NAMES = ["col1", "col2", "col3"] # assume we're given as input or can parse

    create_pdep_stream(FIELD_WIDTH_STREAM_WRAPPER, IDX_MARKER_STREAM_WRAPPER, PACK_SIZE, 
                         TARGET_FORMAT, CSV_COLUMN_NAMES)

