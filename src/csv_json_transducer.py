"""
Contains the "main" method that we call to transduce a field in one formatdsdddd
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
from src.json_converter import JSONConverter


def create_pext_ms(input_file_contents, source_format=TransductionTarget.CSV):
    """Create pext marker stream from the file contents based on the target format.

    Quick-and-dirty python implementation of a simplified character class compiler.
    Search for different characters depending on the target_format. Currently only
    JSON is supported. Remember that bit streams grow from right to left, starting from
    bit position 0. See
    https://github.com/AdamBJ/Python-Prototyping/wiki/Bit-stream-growth-and-processing-order

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
        abc,1234 -> 11110111
    """
    pext_marker_stream = 0
    shift_amnt = 0
    if source_format == TransductionTarget.CSV:
        for character in input_file_contents:
            if character != ',' and character != '\n':
                pext_marker_stream = (1 << shift_amnt) | pext_marker_stream
            shift_amnt += 1
    else:
        raise ValueError("Only CSV transduction is supported.")

    return pext_marker_stream

def verify_user_inputs(pack_size):
    """Verify inputs provided by user."""
    if pack_size == 0 or (pack_size & (pack_size - 1)) != 0:
        # Credit to A.Polino for this check
        raise ValueError("Pack size must be a power of two.")

def main(pack_size, csv_column_names, path_to_file,
         target_format=TransductionTarget.JSON, source_format=TransductionTarget.CSV):
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
    verify_user_inputs(pack_size)

    csv_file_as_str = pablo.readfile(path_to_file)
    pext_marker_stream = create_pext_ms(csv_file_as_str, source_format)
    idx_marker_stream = pablo.create_idx_ms(pext_marker_stream, pack_size)
    field_widths = field_width.calculate_field_widths(pext_marker_stream,
                                                      idx_marker_stream,
                                                      pack_size)
    converter = None
    if target_format == TransductionTarget.JSON:
        # TODO prompt for column names / types here, then extract this block as method
        converter = JSONConverter(field_widths, csv_column_names)
    else:
        raise ValueError("Unsupported target transduction format specified:", target_format)

    pdep_marker_stream = converter.create_pdep_stream()
    json_bp_byte_stream = converter.create_bpb_stream()
    json_bp_bit_streams = [0, 0, 0, 0, 0, 0, 0, 0]
    csv_bit_streams = [0, 0, 0, 0, 0, 0, 0, 0]
    pablo.serial_to_parallel(csv_file_as_str, csv_bit_streams)
    pablo.serial_to_parallel(json_bp_byte_stream, json_bp_bit_streams)
    for i in range(8):
        # Extract bits from CSV bit streams and deposit extracted bits in bp bit streams.
        extracted_bits_stream = pablo.apply_pext(csv_bit_streams[i], pext_marker_stream)
        pablo.apply_pdep(json_bp_bit_streams, i, pdep_marker_stream, extracted_bits_stream)

    output_byte_stream = pablo.inverse_transpose(json_bp_bit_streams, len(json_bp_byte_stream))
    print("input CSV file:", "\n" + csv_file_as_str)
    print("CSV file column names:", csv_column_names)
    print("pext_marker_stream:", bin(pext_marker_stream))
    print("idx_marker_stream:", bin(idx_marker_stream))
    print("field widths:", field_widths)
    print("pdep_marker_stream:", bin(pdep_marker_stream))
    print("output_JSON_file:", "\n" + output_byte_stream)
    #pablo.writefile('out.json', output_byte_stream)
    return output_byte_stream

if __name__ == '__main__':
    main(64, ["col A", "col B", "col C"], "Resources/Test/test_multiline_small.csv")
