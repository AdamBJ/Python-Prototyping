"""Functions used to calculate field widths of fields encoded in field_width_stream_wrapper.

For example, for field_width_stream_wrapper 100010001000, calculate_field_widths
produces [3,3,3], which is a list containg the field widths of the three fields encoded in
the stream (fields are sequences of 0 bits between 1 bits).

Note that the first field width we scan in is width of the *last* field in the input document bc
we process streams from least sig bit to most sig (right to left). This is because it is convienient
to start scans from the least significant bit psn. However, we read documents from left to right
(most sig to least sig), which
means that when it's time to build a stream that represents the output file, we need to start
processing from the field we scanned last. This field is the first field to appear in the file.
"""
from src import pablo

def calculate_field_widths(pext_marker_stream, idx_marker_stream, pack_size):
    """Calculate the field widths of all fields stored in field_width_stream_wrapper.value.

    idx_marker_stream_wrapper.value contains numberBitsInPEXTStream/packSize bits. Set bits in this
    stream correspond to packs in field_width_stream that contain at least a single set bit.
    E.g. if bit 3 of idx_marker_stream_wrapper.value is set, then the third pack in pext_marker_
    stream.value contains at least a single set bit (i.e. it contains field width information
    we need to extract). Assuming pack size is 64, we would need to scan bits 192-256 for fields.

    The basic approach is to first scan through idx_marker_stream_wrapper.value to identify the
    location of a pack with at least a single 1 bit set. We then extract field widths
    from this pack. We continue until we've processed all the packs that contain
    field width information (i.e. until we've processed all set bits in idx_marker_stream).

    E.g. field_width_stream_wrapper.value = 100010001000, idx_marker_stream_wrapper.value = 1,
    output =  [3,3,3]
    """
    # Allows us to simulate pass-by-reference for our streams
    field_width_stream_wrapper = pablo.BitStream(create_field_width_ms(pext_marker_stream))
    idx_marker_stream_wrapper = pablo.BitStream(idx_marker_stream)

    field_widths = []
    field_start = -1
    while idx_marker_stream_wrapper.value:
        non_zero_pack_idx = find_nonzero_pack(idx_marker_stream_wrapper)
        field_start = process_pack(field_width_stream_wrapper, field_widths, field_start,
                                   non_zero_pack_idx, pack_size)
    return field_widths

def find_nonzero_pack(idx_marker_stream_wrapper):
    """Find position of the first set bit in idx_marker stream.

    Find the position, reset the lowest bit of idx_marker_stream_wrapper.value.,
    and return the position.
    """
    non_zero_pack_idx = pablo.count_forward_zeroes(idx_marker_stream_wrapper.value)
    idx_marker_stream_wrapper.value = pablo.reset_lowest_bit(idx_marker_stream_wrapper.value)
    return non_zero_pack_idx

def process_pack(field_width_stream_wrapper, field_widths, field_start, non_zero_pack_idx,
                 pack_size):
    """Extract field widths from pack indexed by non_zero_pack_idx.

    This method scans through the pack in field_width_stream_wrapper.value
    indexed by non_zero_pack to calculate the widths of any fields the pack may contain. We
    perform the calculation by scanning the pack from right to left and counting the number of
    bits that are associated with a field. That means the first field width we calculate is the
    width of the field that appears *last* in the input file.

    Fields are sequences of 0s between 1s, and field start/end positions (1 bits) are not considered part of the fields
    they delineate. For example, 10001 contains a field with width 3 between bits 1 and 3 (inclusive).
    The field end markers are bits 0 and 4. These bits don't count towards the total fw.

    To calculate
    a field width we subtract the absolute position of the field end marker (i.e. the 1 denoting
    the end of the field) from the absolute position of the field start marker. We then subtract 1
    from the resulting value to get the field width (i.e. the number of zeroes between the start
    and end marker of the field).

    Args:
        field_width_stream_wrapper: stream representing the input file. Sequences of 0s stream
            indicate the positions of fields in the input file. E.g. if bits 3-6 are 0s, bytes 3-6
            in the input file belong to a field we want to extract.
        field_widths: list of field widths. Insert new fields into the front of this list
            so that the widths are read from left to right in the same way a human
            would read the field in an input file. The first field width in the final list
            belongs to the first field in the input file, and the last field width belong to the
            last field in the file.
        non_zero_pack_idx: integer indicating which pack of field_width_stream_wrapper we are
            to process.
        pack_size (int): number denoting the width of a pack. Typically 64.
    """
    # Get the pack
    pack_mask = (1 << pack_size) - 1 # e.g. 8 bit mask -> 0...011111111
    aligned_pack_mask = pack_mask << (non_zero_pack_idx * pack_size)
    aligned_pack = aligned_pack_mask & field_width_stream_wrapper.value # got the pack
    pack = aligned_pack >> (non_zero_pack_idx * pack_size)
    pack_wrapper = pablo.BitStream(pack)

    # Process the pack
    abs_pack_start_posn = non_zero_pack_idx * pack_size
    while pack_wrapper.value:
        field_end = pablo.count_forward_zeroes(pack_wrapper.value) + abs_pack_start_posn
        field_widths.insert(0, field_end - field_start - 1)
        field_start = field_end
        pack_wrapper.value = pablo.reset_lowest_bit(pack_wrapper.value)
    return field_start

def create_field_width_ms(pext_marker_stream):
    """Convert pext_marker_stream to field_width_ms.

    pext_marker_streams identifies fields as sequences of 1s. create_pdep_marker_stream
    uses existing pablo functions that scan through sequences of 0s, and so sees fields as
    sequences of 0s bounded by set bits. Another reason to use 0s as field markers
    is that partial field detection is difficult when using 0s as field markers because of
    the infinite number of trailing 0s with Pythons' unlimited precision integers.
    To avoid these problems we need to convert the sequences of 1s in pext_marker_stream
    to sequences of 0s before we pass it into create_pdep_ms.

    First we take this inverse of the stream, then we AND it with a mask that will reset all the
    leading 1 bits that result (except the first leading 1, we need that to denote the end of
    the last field).

    This function should not be neccesary in Parabix because C++ has finite
    precision integers. We should be able to simple take the inverse of the pext_marker_stream.

    Example:
        pext_marker_stream (int): 11011101111, return 100100010001
    """
    temp = pext_marker_stream
    end_of_fields_byte = 0
    while pext_marker_stream:
        end_of_fields_byte += 1
        pext_marker_stream >>= 1
    pext_marker_stream = temp

    field_width_stream = ~pext_marker_stream
    field_width_stream &= (1 << (end_of_fields_byte + 1)) - 1
    # field_width_stream_wrapper += 2 ** (len(csv_file_as_str) + 1) also works! Take away + 1 abv
    return field_width_stream
