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
    pext_marker_stream = create_pext_ms(csv_file_as_str, target_format)
    idx_marker_stream = pablo.create_idx_ms(pext_marker_stream, pack_size)
    field_widths = field_width.calculate_field_widths(pext_marker_stream,
                                                      idx_marker_stream,
                                                      pack_size)
    pdep_marker_stream = create_pdep_stream(field_widths, csv_column_names, target_format)
    json_bp_byte_stream = create_bpb_stream(
        field_widths, len(csv_column_names), csv_column_names)
    json_bp_bit_streams = [0, 0, 0, 0, 0, 0, 0, 0]
    csv_bit_streams = [0, 0, 0, 0, 0, 0, 0, 0]
    pablo.serial_to_parallel(csv_file_as_str, csv_bit_streams)
    pablo.serial_to_parallel(json_bp_byte_stream, json_bp_bit_streams)
    for i in range(8):
        extracted_bits_stream = pablo.apply_pext(csv_bit_streams[i], pext_marker_stream)
        pablo.apply_pdep(json_bp_bit_streams, i, pdep_marker_stream, extracted_bits_stream)

    output_byte_stream = pablo.inverse_transpose(json_bp_bit_streams, len(json_bp_byte_stream))
    #pablo.writefile('out.json', output_byte_stream)
    #print("pext_marker_stream:", bin(pext_marker_stream))
    #print("idx_marker_stream:", bin(idx_marker_stream))
    print("input CSV file:", csv_file_as_str)
    print("CSV file column names:", csv_column_names)
    print("output_JSON_file:", output_byte_stream)
    #print("pdep_marker_stream:", bin(pdep_marker_stream))
    return output_byte_stream

if __name__ == '__main__':
    main(64, ["col A", "col B", "col C"], "Resources/Test/test.csv")
