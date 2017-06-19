import sys
import os
PACKAGE_PARENT = '..'
SCRIPT_DIR = os.path.dirname(os.path.realpath(os.path.join(os.getcwd(),
                                                           os.path.expanduser(__file__))))
sys.path.append(os.path.normpath(os.path.join(SCRIPT_DIR, PACKAGE_PARENT)))

from src.transducer_target_enums import TransductionTarget
from src import pablo
from src.json_converter import JSONConverter

def create_actual_pdep_marker_stream(converter, field_widths, csv_column_names):
    """Return a JSONConvert object."""
    pdep_marker_stream = pablo.BitStream(converter.create_pdep_stream())
    return pdep_marker_stream


def create_expected_pdep_ms(converter, field_widths, csv_column_names):
    """Create pdep marker stream based on the rules programmed into the transducer.

    It can be quite tedious to hand write the pdep marker stream. Sometimes it's easier to
    manually verify the pdep stream created by the program.
    """
    expected_pdep_ms = ""

    starts_or_ends_file = 0
    for i, fw in enumerate(field_widths):
        if i == 0 or i == (len(field_widths) - 1):
            starts_or_ends_file = 1
        else:
            starts_or_ends_file = 0

        preceeding_bpb, following_bpb = converter.get_preceeding_following_bpb(
            i % converter._num_fields_per_unit, starts_or_ends_file)
        expected_pdep_ms = ("0" * following_bpb) + ("1" * fw) + \
            ("0" * preceeding_bpb) + expected_pdep_ms
    return expected_pdep_ms
