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
"""

main()
print(bin(PEXT_MARKER_STREAM))
print(bin(EXTRACTED_BITS_STREAM >> 5))
  # pad the extracted field with JSON bytes. Number of preceeding
        # and following JSON bytes is determined by relative position
        # of CSV value in final JSON object

def main():
    """ Entry point for the program."""
    # Assume we're given the following streams (we need to use Parabix to create them dynamically)
    EXTRACTED_BITS_STREAM = int('1110011', 2)
    PEXT_MARKER_STREAM = int('0110110110', 2)
    IDX_MARKER_STREAM = 0

    PDEP_MARKER_STREAM = 0

    field_widths = calculate_field_widths(PEXT_MARKER_STREAM, IDX_MARKER_STREAM)
    fieldType = 0

    for field_width in field_widths:
        # extract field_width bits from extracted_bits_stream
        extracted_field = extract_field(EXTRACTED_BITS_STREAM, field_width)

        # pad CSV value with JSON boilerplate
        transduced_field = transduce_field(extracted_field, fieldType)

        # AND padded value into PDEP marker stream
        insert_field(transduced_field, PDEP_MARKER_STREAM)

def calculate_field_widths(pext_marker_stream, idx_marker_stream):
    """
    Calculate the field widths of all fields stored in pext_marker_stream.

    We assume we're given an idx_marker_stream that tells us which (usually 64-bit)
    packs contain at least a single 1 bit. Since the idx_marker_stream is calculated
    using SIMD instructions we don't have an easy way to calculate it outside of the
    Parabix framework.

    E.g. input of pext_marker_stream = 11101110111, idx_marker_stream 1, output 3,3,3
    """
    while idx_marker_stream:
        # scan through idx_marker_stream to identify location of a pack with at least a single 1 bit set

        # load the pack and scan through it to determine the widths of the fields it contains

        # store state information
    
    return field_widths