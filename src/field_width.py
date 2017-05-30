"""
Contains functions used to calculate the field widths of fields encoded in a marker bit stream.
For example, for the marker bit stream 00010001000, calculate_field_widths can be called
to produce [3,3,3], which is a list containg the field widths of the three fields encoded in
the stream (fields are sequences of 0 bits).
"""
from src import pablo

def calculate_field_widths(pext_marker_stream_wrapper, idx_marker_stream_wrapper, pack_size):
    """
    Calculate the field widths of all fields stored in pext_marker_stream_wrapper.value.

    idx_marker_stream_wrapper.value contains numberBitsInPEXTStream/packSize bits. Set bits in this
    stream correspond to packs in pext_marker_stream that contain at least a single set bit.
    E.g. if bit 3 of idx_marker_stream_wrapper.value is set, then the third pack in pext_marker_
    stream.value contains at least a single set bit (i.e. it contains field width information
    we need to extract).

    The basic approach is to first scan through idx_marker_stream_wrapper.value to identify the
    location of a pack with at least a single 1 bit et. We then extract field widths
    from this pack. We continue until we've processed all the packs that contain
    field width information.

    E.g. pext_marker_stream_wrapper.value = 00010001000, idx_marker_stream_wrapper.value = 1,
    output =  [3,3]
    """
    if pext_marker_stream_wrapper.value < 0 or idx_marker_stream_wrapper.value < 0 or pack_size < 0:
        raise ValueError("Input streams cannot be represented by negative integers.")
    elif pack_size == 0 or (pack_size & (pack_size - 1)) != 0:
        # Credit to A.Polino for this check
        raise ValueError("Pack size must be a power of two.")

    field_widths = []
    field_start = -1
    while idx_marker_stream_wrapper.value:
        non_zero_pack_idx = find_nonzero_pack(idx_marker_stream_wrapper)
        field_start = process_pack(pext_marker_stream_wrapper, field_widths, field_start,
                                   non_zero_pack_idx, pack_size)
    return field_widths

def find_nonzero_pack(idx_marker_stream_wrapper):
    """
    Find the first set bit in idx_marker stream and return its location, then
    reset the lowest bit of idx_marker_stream_wrapper.value.
    """
    non_zero_pack_idx = pablo.count_leading_zeroes(idx_marker_stream_wrapper.value)
    idx_marker_stream_wrapper.value = pablo.reset_lowest_bit(idx_marker_stream_wrapper.value)
    return non_zero_pack_idx

def process_pack(pext_marker_stream_wrapper, field_widths, field_start, non_zero_pack_idx,
                 pack_size):
    """
    Extract field widths from pack in pext_marker_stream_wrapper.value indexed by non_zero_pack_idx.

    This method scans through the pack in pext_marker_stream_wrapper.value
    indexed by non_zero_pack to calculate the widths of any fields the pack may contain. We
    perform the calculation by scanning the pack from right to left and counting the number of
    bits that are associated with a field.

    Fields are sequences of 0s between 1s. To calculate
    a field width we subtract the absolute position of the field end marker (i.e. the 1 denoting
    the end of the field) from the absolute position of the field start marker. We then subtract 1
    from the resulting value to get the field width (i.e. the number of zeroes between the start
    and end marker of the field).
    """
    pack_mask = (1 << pack_size) - 1 # e.g. 8 bit mask -> 0...011111111
    aligned_pack_mask = pack_mask << (non_zero_pack_idx * pack_size)
    aligned_pack = aligned_pack_mask & pext_marker_stream_wrapper.value
    pack = aligned_pack >> (non_zero_pack_idx * pack_size)
    pack_wrapper = pablo.IntWrapper(pack)
    while pack_wrapper.value:
        field_end = pablo.count_leading_zeroes(pack_wrapper.value) + (non_zero_pack_idx * pack_size)
        field_widths.append(field_end - field_start - 1)
        field_start = field_end
        pack_wrapper.value = pablo.reset_lowest_bit(pack_wrapper.value)
    return field_start
