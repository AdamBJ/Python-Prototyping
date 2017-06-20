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
cwd = os.getcwd()
t2 = os.path.expanduser(__file__)
t3 = os.path.realpath(os.path.join(os.getcwd(),
                                                           os.path.expanduser(__file__)))
SCRIPT_DIR = os.path.dirname(os.path.realpath(os.path.join(os.getcwd(),
                                                           os.path.expanduser(__file__))))
sys.path.append(os.path.normpath(os.path.join(SCRIPT_DIR, PACKAGE_PARENT)))
test = os.path.normpath(os.path.join(SCRIPT_DIR, PACKAGE_PARENT))
from src.transducer_target_enums import TransductionTarget, SourceFormats
from src import pablo
from src import field_width
from src.json_converter import JSONConverter

def main(pack_size, csv_column_names, path_to_file,
         target_format=TransductionTarget.JSON, source_format=SourceFormats.CSV):
    """Accept path to file in source_format, transduces file to target_format.

    Args:
        pack_size: size of fundamental stream processing unit. The real version of PDEP and PEXT
            can process pack_size bits at a time (our quick and dirty versions process the entire
            stream in one go, though).
            create_idx_ms and the field width functions process streams pack_size bits at once.
        csv_column_names: TODO refactor. This input should be requested within pdep_stream_gen if
            transduction target == JSON.
        path_to_file(str): path to file to transduce. Absolute, or relative to the main
            project directory.
        target_format: The format we want to transduce file at path_to_file to.
        source_format: The format of the file at path_to_file.
    Returns:
        The transduced file. E.g. for CSV to JSON, the JSON file that results from transducing
            the input CSV file.
    """

    # Process the input file
    csv_file_as_str = pablo.readfile(path_to_file)
    # TODO replace [] with format, e.g CSV
    field_widths = field_width.calculate_field_widths(csv_file_as_str, pack_size, [",", "\n"])
    fields_pext_ms = pablo.create_pext_ms(csv_file_as_str, [",", "\n"], True)

    # Create the Converter object we'll use to transduce the file
    converter = None
    if target_format == TransductionTarget.JSON:
        # TODO prompt for column names / types here, then extract this block as method
        converter = JSONConverter(field_widths, csv_column_names)
    else:
        raise ValueError("Unsupported target transduction format specified:", target_format)
    converter.verify_user_inputs(pack_size, csv_file_as_str)

    pdep_marker_stream = converter.create_pdep_stream()
    json_bp_byte_stream = converter.create_bpb_stream()
    json_bp_bit_streams = [0, 0, 0, 0, 0, 0, 0, 0]
    csv_bit_streams = [0, 0, 0, 0, 0, 0, 0, 0]
    extracted_bit_streams = [0, 0, 0, 0, 0, 0, 0, 0]
    # Decompose the input bytestream and output byte stream template into parallel bit streams
    pablo.serial_to_parallel(csv_file_as_str, csv_bit_streams)
    pablo.serial_to_parallel(json_bp_byte_stream, json_bp_bit_streams)

    # Transduce
    for i in range(8):
        # Extract bits from CSV bit streams and deposit in bp bit streams.
        extracted_bit_streams[i] = pablo.apply_pext(csv_bit_streams[i], fields_pext_ms)
        pablo.apply_pdep(json_bp_bit_streams, i, pdep_marker_stream, extracted_bit_streams[i])

    # Combine the transduced parallel bit streams into the final output byte stream
    output_byte_stream = pablo.inverse_transpose(json_bp_bit_streams, len(json_bp_byte_stream)) # Unicode str in Python 3.x
    #extracted_byte_stream = pablo.inverse_transpose(extracted_bit_streams, sum(field_widths))
    print("input CSV file:", "\n" + csv_file_as_str)
    print("CSV file column names:", csv_column_names)
    print("fields_pext_ms:", bin(fields_pext_ms))
    print("field widths:", field_widths)
    print("pdep_marker_stream:", bin(pdep_marker_stream))
    print("output_JSON_file:", "\n" + output_byte_stream)
    #pablo.writefile('out.json', output_byte_stream)
    return output_byte_stream

if __name__ == '__main__':
    #main(64, ["id","first_name","last_name","email","gender","ip_address"], "Resources/Test/test_multiline_big.csv")
    #main(64, ["col A"], "Resources/Test/s2p_test.csv")
    main(64, ["onesy", "two", "flap!"], "Resources/Test/test.csv")
