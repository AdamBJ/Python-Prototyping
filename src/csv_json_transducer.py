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
from src.pdep_stream_gen import generate_pdep_stream

def main(pack_size, csv_column_names):
    #.encode() goes from a Unicode string to equivalent Unicode bytes
    csv_byte_stream = int.from_bytes(pablo.readfile("Resources/test.csv").encode('utf-8'), 'big')

    # print(csv_byte_stream)
    # print(bin(csv_byte_stream))
    # print(bin(int.from_bytes(csv_byte_stream.encode(), 'little')))
    # for byte in pablo.readfile("Resources/test.csv"):   
    #     print(bin(int.from_bytes(byte.encode(), 'big')))
    #     print(ord(byte))

     idx_marker_stream = create_idx_ms()
    # pext_marker_stream = create_pext_ms()
    # pdep_marker_stream = generate_pdep_stream(!!field_width_stream_wrapper!!, idx_marker_stream,
    #                                           pack_size, TransductionTarget.json, csv_column_names)
    # json_bp_byte_stream = create_bpb_stream(TransductionTarget.json)

    # json_bp_bit_streams = pablo.BasisBits()
    # csv_bit_streams = pablo.BasisBits()
    # json_output_bit_streams = pablo.BasisBits()
    # pablo.transpose_streams(json_bp_byte_stream, json_bp_bit_streams)
    # pablo.transpose_streams(csv_byte_stream, csv_bit_streams)

    # for i in range(8):
    #     extracted_bits_stream = apply_pext(ith CSV stream, pext_marker_stream)
    #     apply_pdep(ith json bp stream, pdep_marker_stream)
    
    # output_byte_stream = pablo.inverse_transpose(json_output_bit_streams, LENGTH?)
    # pablo.writefile(...)
    # return output_byte_stream

if __name__ == '__main__':
    main(64, ["col1", "col2", "col3"])
