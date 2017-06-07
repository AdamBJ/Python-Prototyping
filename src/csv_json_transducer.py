"""
Contains the "main" method that we call to transduce a field in one format
to a field in another format. Currently the only transduction operation 
supported is CSV to JSON.
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
from src.pdep_stream_gen import create_pdep_stream
from src import field_width


def create_pext_ms(input_file_contents, target_format=TransductionTarget.JSON):
    """Create pext marker stream from the file contents based on the target format.

    Quick-and-dirty python implementation of a simplified character class compiler.
    Search for different characters depending on the target_format. Currently only
    JSON is supported.

    JSON: Scan the input file bytestream for instances of non-delimiter characters
    (i.e. anything that's not ',').

    Args:
        target_format (enum): The format of the output final file (e.g. JSON for CSV to JSON).
        input_file_contents (str): Contents of the input file (e.g. CSV file for CSV to JSON).
    Return:
        pext_marker_stream: A bit stream identifying the locations of bits in the input file
        we want the PEXT operation to extract.
    Example:
        Input file is abc,123,def and target == JSON, pext_marker_stream would be 11101110111.
    """
    pext_marker_stream = 0
    if target_format == TransductionTarget.JSON:
        for character in input_file_contents:
            pext_marker_stream <<= 1
            if character != ',':
                pext_marker_stream |= 1
    else:
        raise ValueError("Only CSV to JSON transduction is supported.")

    return pext_marker_stream

# TODO add this to pablo.py for others to use?


def create_idx_ms(marker_stream, pack_size):
    """Scan through marker_stream to identify packs with set bits.

    Quick-and-dirty python implementation of idxMarkerStream Parabix kernel.
    Whereas the Parabix kernel using simd operations to generate the idx
    stream quickly, this function simply does a sequential scan of the input
    stream to identify packs of interest.

    Args:
        marker_stream (int): The stream to scan through.
        pack_size (int): The final index stream is marker_stream length / pack_size
            bits long. Each bit of the index stream represents a pack_size "pack"
            of input stream bits. If the index stream bit is set, that means the
            pack contains at least one set bit and requires processing.
    Returns:
        idx_marker_stream (int): See pack_size comment.
    """
    idx_marker_stream = 0
    pack_mask = (1 << pack_size) - 1

    while marker_stream:
        non_empty_pack = pablo.any(pack_mask & marker_stream)
        marker_stream >>= pack_size
        idx_marker_stream <<= 1
        idx_marker_stream = idx_marker_stream | non_empty_pack

    return idx_marker_stream


def create_field_width_ms(pext_marker_stream, input_file_length):
    """Convert pext_marker_stream to field_width_ms.

    pext_marker_streams identifies fields as sequences of 1s. create_pdep_marker_stream
    uses existing pablo functions that scan through sequences of 0s, and so sees fields as
    sequences of 0s. That means we need to convert the sequences of 1s in pext_marker_stream
    to sequences of 0s before we pass it into create_pdep_ms.

    First we take this inverse of the stream, then we AND it with a mask that will reset all the
    leading 1 bits that result (except the first leading 1, we need that to denote the end of
    the last field).

    Note that this function may not be neccesary in the final Parabix version. We can simply
    add support to Pablo for scanning sequences of 1s.

    Example:
        pext_marker_stream (int): 11011101111, return 100100010001
    """
    field_width_stream_wrapper = ~pext_marker_stream
    field_width_stream_wrapper &= (1 << (input_file_length + 1)) - 1
    # field_width_stream_wrapper += 2 ** (len(csv_file_as_str) + 1) also works! Take away + 1 abv
    return field_width_stream_wrapper

# TODO remove csv_column_names


def create_bpb_stream(field_widths, num_fields_per_unit, csv_column_names, target=TransductionTarget.JSON):
    """Create boilerplate byte stream.

    The boilerplate byte stream is a stream of boilerplate characters with
    space added for values extracted from the input file (e.g. CSV values).
    The input file values will be inserted into the stream later by PDEP operations.

    Args:
        num_fields_per_unit: The number of fields in a "unit" of output. For example,
        output units for JSON are JSON objects that contain num_fields_per_unit fields.
        For CSV files, num_fields_per_unit is just one, since a CSV unit is whatever's
        between two delimiters.

    Example:
        For a CSV input file with a single value: [{\n"columnName": ___\n}]
        Where __ is long enough to accommodate all the characters of the CSV field we
        extract from the CSV file.
    """
    if target == TransductionTarget.JSON:
        # TODO prompt for column names
        # quick and dirty concatenation
        #json_bp_byte_stream = "["
        json_bp_byte_stream = ""
        field_type = 0
        for i, fw in enumerate(field_widths):
            if field_type == 0:
                # first field in JSON object
                json_bp_byte_stream += "{\n"

            json_bp_byte_stream += csv_column_names[field_type]
            json_bp_byte_stream += ": "
            json_bp_byte_stream += "_" * fw  # space for CSV value. >>= field_width?

            if field_type == (num_fields_per_unit - 1):
                # final field
                json_bp_byte_stream += "\n}"
                field_type = -1  # reset
                # can probably do this check in Pbix by comparing produced/processed count
                # if i == len(field_widths) - 1:
                #json_bp_byte_stream += ","
            else:
                # first or middle field
                json_bp_byte_stream += ",\n"

            field_type += 1
        #json_bp_byte_stream += "]"

        return json_bp_byte_stream

    else:
        raise ValueError(
            "Only CSV to JSON transduction is currently supported.")


def apply_pext(bit_stream, pext_marker_stream, field_widths):
    """Apply quick-and-dirty python version of PEXT to bit_stream.

    We process bit_stream from right to left, but we read it (as humans) from left to right.
    Therefore, the last field we processes should appear first in the resulting
    extracted bit stream.

    Args:
        bit_stream: The bit stream we'll extract bits from.
        pext_marker_stream: The marker stream that tells us which bit positions in bit_stream
            to extract bits from.
        field_widths: List that tells us the width of each field that will be extracted from
            the bit_stream. Fields are sequences of 1 bits in pext_marker_stream.
    Returns:
        extracted_bit_stream: The bit stream that results from extracting bits from bit_stream
            at the locations indicated by pext_marker_stream.
    Example:
        bit_stream:         1010001110
        pext_marker_stream: 1111110001
        field_widths: [6,1]

        extracted_bit_stream: 1010000
    """
    extracted_bit_stream = 0
    shift_amnt = 0
    #print(bin(bit_stream))
    #print(bin(pext_marker_stream))
    for fw in reversed(field_widths):
        leading_zeroes = pablo.count_leading_zeroes(pext_marker_stream)
        field = (1 << fw) - 1
        field <<= leading_zeroes
        field &= bit_stream
        field >>= leading_zeroes # field now contains extracted bit stream data

        # reset pext_marker_stream bits belonging to the field we just processed
        pext_marker_stream = pext_marker_stream & ~((1 << (leading_zeroes + fw)) - 1)
        extracted_bit_stream = (field << shift_amnt) | extracted_bit_stream
        shift_amnt += fw
    return extracted_bit_stream

def apply_pdep(bp_bit_streams, bp_stream_idx, pdep_marker_stream, extracted_bits_stream,
               field_widths):
    """Apply quick-and-dirty Python version of pdep to bp_bit_streams[bp_stream_idx].

    Like apply pext, we process streams from right to left but read them from left to right.
    field_widths maintains the left to right orientation, so we go through it backwards.

    Args:
        bp_bit_streams: Stream set that contains the stream the pext operation will be applied on.
        bp_stream_idx: Index in bp_bit_streams of the stream we will apply the pdep operation to.
        pdep_marker_stream: Marker stream that tells us the positions within bp_bit_streams[bp_stream_idx]
            that the extracted bits should be deposited.
        extracted_bits_stream: The stream we will be inserting into bp_bit_streams[bp_stream_idx] at the
            locations indicated by pdep_marker_streams. Consists of extracted field bits, e.g. for a CSV file
            extracted_bits_stream could be obtained by applying s2p on a version of the CSV file with the
            delimiters stripped out (we use a PEXT operation, though).
        field_widths: the widths of the fields contained in extracted_bits_stream. Remeber that extracted_
        bits_stream is a bit stream, not a byte stream. We need to combine 8 extracted_bits_streams using
        p2s to obtain the fields byte stream.
    Example:
        extracted_bits_stream =        101011
        pdep_marker_stream =           000000001110000000011100000000
        bp_bit_stream[bp_stream_idx] = 000000000000000000000000000000

        bp_bit_stream[bp_stream_idx] = 000000001010000000001100000000
    """
    for fw in reversed(field_widths):
        leading_zeroes = pablo.count_leading_zeroes(pdep_marker_stream)
        field = ((1 << fw) - 1)
        bp_bit_streams[bp_stream_idx] &= ~(field << leading_zeroes) # zero out deposit field
        field &= extracted_bits_stream
        extracted_bits_stream >>= fw
        bp_bit_streams[bp_stream_idx] |= (field << leading_zeroes)

        # reset pdep_marker_stream bits belonging to the field we just processed
        pdep_marker_stream = pdep_marker_stream & ~((1 << (leading_zeroes + fw)) - 1)

def main(pack_size, csv_column_names, path_to_file, target_format=TransductionTarget.JSON,
         source_format=TransductionTarget.CSV):
    """Accept path to file in source_format and pack_size. Transduces file to target_format.

    Args:
        pack_size: size of fundamental stream processing unit. The real version of PDEP and PEXT
            can process pack_size bits at a time (our quick and dirty versions process the entire
            stream in one go, though).
            create_idx_ms and the field width functions process streams pack_size bits at once.
        csv_column_names: TODO refactor. This input should be requested within pdep_stream_gen if
            transduction target == JSON.
        path_to_file(str): path to file to transduce. Absolute, or relative to the main
            project directory.
    Returns:
        The transduced file. E.g. for CSV to JSON, the JSON file that results from transducing
            the input CSV file.
    """
    if pack_size == 0 or (pack_size & (pack_size - 1)) != 0:
        # Credit to A.Polino for this check
        raise ValueError("Pack size must be a power of two.")

    csv_file_as_str = pablo.readfile(path_to_file)
    pext_marker_stream = create_pext_ms(target_format, csv_file_as_str)
    idx_marker_stream = create_idx_ms(pext_marker_stream, pack_size)
    field_width_marker_stream = create_field_width_ms(pext_marker_stream, len(csv_file_as_str))
    field_widths = field_width.calculate_field_widths(pablo.BitStream(pext_marker_stream),
                                                      pablo.BitStream(idx_marker_stream),
                                                      pack_size)
    pdep_marker_stream = create_pdep_stream(field_widths, csv_column_names, target_format)
    json_bp_byte_stream = create_bpb_stream(
        target_format, field_widths, len(csv_column_names), csv_column_names)
    json_bp_bit_streams = [0, 0, 0, 0, 0, 0, 0, 0]
    csv_bit_streams = [0, 0, 0, 0, 0, 0, 0, 0]
    pablo.serial_to_parallel(csv_file_as_str, csv_bit_streams)
    pablo.serial_to_parallel(json_bp_byte_stream, json_bp_bit_streams)
    for i in range(8):
        extracted_bits_stream = apply_pext(csv_bit_streams[i], pext_marker_stream, field_widths)
        apply_pdep(json_bp_bit_streams, i, pdep_marker_stream, extracted_bits_stream, field_widths)

    output_byte_stream = pablo.inverse_transpose(json_bp_bit_streams, len(json_bp_byte_stream))
    #pablo.writefile('out.json', output_byte_stream)
    # return output_byte_stream

    print("output_byte_stream:\n", output_byte_stream)
    print("pext_marker_stream:", bin(pext_marker_stream))
    print("idx_marker_stream:", bin(idx_marker_stream))
    print("field_width_marker_stream:", bin(field_width_marker_stream))
    print("pdep_marker_stream:", bin(pdep_marker_stream))

if __name__ == '__main__':
    main(64, ["col1"], "Resources/Test/s2p_test.csv")
