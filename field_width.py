"""
Contains functions used to calculate the field widths of fields encoded in a marker bit stream.
For example, for the marker bit stream 11101110111, calculate_field_widths can be called
to produce [3,3,3], which is a list containg the field widths of the three fields encoded in
the stream (fields are sequences of 1 bits).
"""
import pablo

def calculate_field_widths(pext_marker_stream, idx_marker_stream):
    """
    Calculate the field widths of all fields stored in pext_marker_stream.

    We assume we're given an idx_marker_stream that tells us which
    packs in the pext stream contain at least a single 1 bit. A pack is a (usually)
    64-bit segment of the pext_marker_stream.

    idx_marker_stream contains numberBitsInPEXTStream / packSize bits. Bits in this
    stream that are set correspond to packs that contain at least a single set bit,
    e.g. if bit 3 of idx_marker_stream is set, then the third pack in pext_marker_
    stream contains at least a single set bit (i.e. it contains field width information
    we need to extract).

    The basic approach is to first scan through idx_marker_stream to identify the
    location of a pack with at least a single 1 bit et. We then extract field widths
    from this pack. We continue until we've processed all the packs that contain
    field width information.

    E.g. input of pext_marker_stream = 11101110111, idx_marker_stream 1, output 3,3,3
    """
    field_widths = []
    state = [1, 0]
    while idx_marker_stream:
        non_zero_pack_idx = pablo.count_leading_zeroes(idx_marker_stream)
        idx_marker_stream = pablo.AdvancebyPos(idx_marker_stream, non_zero_pack_idx + 1)
        pext_marker_stream = process_pack(pext_marker_stream, field_widths, state,
                                          non_zero_pack_idx)
    return field_widths

def process_pack(pext_marker_stream, field_widths, state, non_zero_pack_idx):
    """
    Extract field widths from the pack in pext_marker_stream indexed by non_zero_pack_idx.

    This method scans through the pack in pext_marker_stream
    indexed by non_zero_pack to calculate the widths of any fields the pack may contain. We
    perform the calculation by scanning the pack from right to left and counting the number of
    bits that are associated with a field.

    E.g. pext_marker_stream = ... 111.111.111. First take the inverse of
    the stream, then we use the count_leading_zeroes function to determine the width of the first
    field. Record this information, clear the lowest three bits, and scan again to find the width
    of the second field. The process repeats until the pack has been scanned completely.

    The tricky part is that we may have a field that spans multiple packs. To handle this, we
    maintain state information that tells us: a) whether we're looking for the start of a field or
    the end and b) the width of partially scanned fields. state[0] tells us if we're looking for
    a field start or end position and state[1] tells us the width of any partially scanned field.
    """
    looking_for_field_start = state[0]
    partial_field_width = state[1]

    pext_marker_stream = pablo.AdvancebyPos(pext_marker_stream, non_zero_pack_idx * PACK_SIZE)
    if looking_for_field_start:
        
        pass
    else:
        field_width = pablo.count_leading_zeroes(~pext_marker_stream)
        field_widths.append(field_width + partial_field_width)
        pass
    
    return pext_marker_stream