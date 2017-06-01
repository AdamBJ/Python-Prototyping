""" Contains functions used for pdep marker stream geneartion."""
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

def generate_pdep_stream(pext_marker_stream, idx_marker_stream, pack_size,
                         target_format, csv_column_names):
    """Generate a bit mask stream for use with the PDEP operation.

    Takes as input a PEXT marker stream and index marker stream and produces a PDEP
    marker stream. The PDEP marker stream shows where in an output file the extracted
    bits should be inserted in order to complete the desired transduction operation.

    All streams are represented as (unbounded) integers. BitStream is provided as a
    means of passing these integers "by reference". Doing so allows us to make changes
    to our "stream" variables inside methods and have these changes persist once the
    methods return.

    Args:
        pext_marker_stream: Marks the locations of bits we want the PDEP operation
            to extract from the CSV file. Used in generate_pdep_stream to calculate field widths,
            not for PEXT operations
        idx_marker_stream: Tells us which packs (clusters of pack_size bits) in pext_marker_stream
            contain at least a single set bit.
        pack_size: Integer describing the width of a pack.
        target_format: E.g. JSON, CSV, ...
        csv_column_names: Names of the columns in the input CSV file (if target_format == JSON)


    Returns (BitStream):
        The pdep bit stream.

    Examples:
        >>> generate_pdep_stream(11101110111, 1, 64, TransductionTarget.JSON,
                                 ["col1", "col2", "col3"])
        1879277596

        1879277596 when viewed as a bit stream is ..........111..........111..........111..
    """
    print(bin(pext_marker_stream.value))
    pdep_marker_stream = pablo.BitStream(0)
    field_widths = calculate_field_widths(pext_marker_stream, idx_marker_stream, pack_size)
    field_type = 0
    for field_width in field_widths:
        field_wrapper = pablo.BitStream((1 << field_width) - 1) # create field
        num_boilerplate_bytes = transduce_field(field_wrapper, field_type, target_format,
                                                csv_column_names)
        insert_field(field_wrapper, pdep_marker_stream, num_boilerplate_bytes + field_width)
        field_type += 1
        if field_type == len(csv_column_names):
            field_type = 0
    print(bin(pdep_marker_stream.value))
    return pdep_marker_stream.value

# TODO check handles non-ASCII encodings (everything but UTF-16 should work)
# TODO remove csv_column_names as argument. Only needed if target is CSV. Parse/prompt
# user if required
def transduce_field(field_wrapper, field_type, target, csv_column_names):
    """ Pad extracted field with appropriate boilerplate.

    Args:
        field_wrapper: The field to trasduce.
        field_type: A scalar describing the type of the field
        csv_column_names: The names of the columns in the input CSV file
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
        preceeding_boilerplate_bytes = 4 + len(csv_column_names[field_type])
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

def insert_field(field_wrapper, pdep_marker_stream, transduced_field_width):
    """AND padded value into PDEP marker stream."""
    pdep_marker_stream.value = (pdep_marker_stream.value << transduced_field_width) \
                               | field_wrapper.value

if __name__ == '__main__':
    # Assume we're given the following streams (we need to use Parabix to create them dynamically)
    PEXT_MARKER_STREAM = pablo.BitStream(int('100010001000', 2))
    IDX_MARKER_STREAM = pablo.BitStream(1)
    PACK_SIZE = 64 #user can optionally specify
    TARGET_FORMAT = TransductionTarget.JSON # this is the only user-provided value?
    CSV_COLUMN_NAMES = ["col1", "col2", "col3"] # assume we're given as input or can parse

    generate_pdep_stream(PEXT_MARKER_STREAM, IDX_MARKER_STREAM, PACK_SIZE, TARGET_FORMAT,
                         CSV_COLUMN_NAMES)

