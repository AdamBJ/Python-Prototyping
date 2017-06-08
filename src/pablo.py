#
# pablo.py
#
# Bitstream Utilities
#
# These are quick-and-dirty Python implementations of utilities
# for demo purposes only.
#
# Dan Lin, Robert D. Cameron
# March 24, 2012
#
#----------------------------------------------------------------------------
# 
# We use python's unlimited precision integers for unbounded bit streams.
# This permits simple logical operations on the entire stream.
# Assumption: bitstreams are little-endian (e.g., as on x86).
#
#----------------------------------------------------------------------------
# 
import sys
import codecs
# Utility functions for demo purposes.

EOF_mask = 0
data = ''

def writefile(filename, s, charcode='utf-8',):
    f = codecs.open(filename, 'w', encoding=charcode)
    f.write(s) # writes encoded bytes to file
    f.close()

def readfile(filename):
    f = open(filename, 'r')
    contents = f.read()
    f.close()
    return contents

def transpose_streams(s, b):
    mask = 128
    index = 0
    #global data
    while index < 8:
        current = 0
        cursor = 1
        for byte in s:
            if  (ord(byte) & mask != 0):
                current += cursor
            cursor <<= 1

        index+=1
        mask>>=1
        if index == 1:
            b.bit_0 = current
        elif index == 2:
            b.bit_1 = current
        elif index == 3:
            b.bit_2 = current
        elif index == 4:
            b.bit_3 = current
        elif index == 5:
            b.bit_4 = current
        elif index == 6:
            b.bit_5 = current
        elif index == 7:
            b.bit_6 = current
        elif index == 8:
            b.bit_7 = current
    #data  = s
    return cursor-1  # EOF mask

def match(s,marker):
    pos = count_leading_zeroes(marker)
    i = 0
    for byte in s:
        if byte != data[pos+i]:
            return 0
        i +=1
    return marker

def inFile(lex_error):
    return EOF_mask & lex_error

def atEOF(strm):
    if strm > EOF_mask or strm < 0:
        return EOF_mask + 1
    else:
        return 0

def count_leading_zeroes(strm):
    zeroes = 0
    while (strm & 0xFFFFFFFF) == 0:
        zeroes += 32
        strm >>= 32
    while (strm & 1) == 0:
        zeroes += 1
        strm >>= 1
    return zeroes
#
#
#  Are there any bits in a stream
def any(strm):
    return strm != 0
#
#  ScanThru is the core operation for parallel scanning.
#  Given a bitstream Cursors marking current cursor positions,
#  advance each cursor through occurrences of a character
#  class identified by ScanStream.

def ScanThru(Cursors, ScanStream):
    return (Cursors + ScanStream) & ~ScanStream

def ScanTo(Cursors, ToStream):  
    ScanStream = ~ToStream & EOF_mask
    return (Cursors + ScanStream ) &~ ScanStream

def ScanToFirst(ScanStream):
    return ScanTo(1, ScanStream)
#
# Advance all cursors by one position.
def Advance(stream):
    return stream + stream

def AdvancebyPos(stream, pos):
    return stream >> pos


#
# Advance-and-Scan
#
# These are common and useful operations, with a slight
# optimization to avoid a separate Advance.   
#
# Warning:  they each require a "separation" property
# between markers and scanclass streams.   This property
# normally seems to hold in our applications, but we should
# work out methods for formally establishing it.
#
# AdvanceThenScanThru(marker, scanclass)
# optimizes: ScanThru(Advance(marker), scanclass)
# requires: scanclass-marker separation: no occurrence of
# a marker immediately after a scanclass run.  That is,
# any(Advance(scanclass) & marker) == False
# (this is a sufficient precondition, but maybe not weakest.)
#
def AdvanceThenScanThru(marker, scanclass):	
    return ScanThru(marker, marker | scanclass)
#
# AdvanceThenScanTo(marker, scanclass)
# optimizes: ScanTo(Advance(marker), scanclass)
# requires: scanclass existence between markers

def AdvanceThenScanTo(marker, scanclass):
    #return ScanTo(Advance(marker), scanclass)
    charclass = ~scanclass & EOF_mask
    return (marker + (charclass | marker)) &~ charclass

#
# Span operations
# These operations define spans of positions from
# starting to ending positions.
# Requirement:  start and end positions are properly
# matched: popcount(ends)==popcount(starts) and
# nth-one(starts, n) < nth-one(ends, n), if n < popcount(starts)
# nth-one(ends, n) < nth-one(starts, n+1) if n < popcount(starts)

def SpanUpTo(starts, ends):
    return (ends - starts)

def InclusiveSpan(starts, ends): 
    return (ends - starts) | ends

def ExclusiveSpan(starts, ends): 
    return (ends - starts) &~ starts

#Functions copied from bitutil.py

def extract_bit(strm, pos):
    bit = (strm >> pos) & 1
    return bit

def filter_bits(bitstream, delmask):
    newstream = 0
    cursor = 1
    while delmask > 0:
        if delmask & 1 == 0:
            if bitstream & 1 == 1:
                newstream += cursor
            cursor += cursor
        delmask >>= 1
        bitstream >>= 1
    while bitstream > 0:
        if bitstream & 1 == 1:
            newstream += cursor
        cursor += cursor
        bitstream >>= 1
    return newstream

def filter_bytes(bytestream, delmask):
    newstream=""
    cursor = 1
    for c in bytestream:
        if delmask & cursor == 0:
            newstream += c
        cursor += cursor
    return newstream

def merge_bytes(stream1, stream2):
    s = ""
    for i in range(len(stream1)):
        s += stream1[i]
        s += stream2[i]
    return s

def bitstream2string(stream, lgth):
    str = ""
    for i in range(lgth):
        if stream & 1 == 1: str += '1'
        else: str += '.'
        stream >>= 1
    return str

def bitstream2stringLE(stream, lgth):
    str = ""
    for i in range(lgth):
        if stream & 1 == 1: str = '1' + str
        else: str = '.' + str
        stream >>= 1
    return str

def print_aligned_streams(stream_list):
    """Print out a set of aligned streams."""
    label_max = max([len(p[0]) for p in stream_list])
    for p in stream_list:
        print (p[0] + " "*(label_max - len(p[0]))) + ": " + p[1]

def latex_streams(stream_list):
    """Return a latex table for streams."""
    table = "\\begin{tabular}{cr}"
    for p in stream_list:
        table += "\\\\\n" + p[0] +" & \\verb`" + p[1] +"`"
    return table + "\n\\end{tabular}\n"

def print_aligned_u8_byte_streams(u8_byte_stream):
    """Print out a set of 'encoding' aligned streams."""

    # Set the system info to print utf-8
    info = codecs.lookup('utf-8')
    sys.stdout = info.streamwriter(sys.stdout)

    label_max = max([len(p[0]) for p in u8_byte_stream])
    for p in u8_byte_stream:
        sys.stdout.write(p[0] + " "*(label_max - len(p[0])) + ": ")

        for c in (p[1].decode('utf-8')): 		 	# for each unicode character
            u8_seq_len = len(c.encode('utf-8')) 	# encode the unicode character as utf-8 and get the utf-8 sequence length
            if u8_seq_len == 1:
                sys.stdout.write(c)
            elif u8_seq_len == 2:
                sys.stdout.write('2_')  			# align 2 byte sequences with a trailing _
            elif u8_seq_len == 3:
                sys.stdout.write('3__')				# align 3 byte sequences with 2 trailing _
            elif u8_seq_len == 4:
                sys.stdout.write('4___')			# align 2 byte sequences with 3 trailing _
            else:
                sys.stdout.write('Error: Invalid UTF-8 character sequence of length: ' + str(u8_seq_len) + '.')
        sys.stdout.write('\n')

def high_nybble_stream(bytes):
    str=""
    for b in bytes:
        h = hex(ord(b))[-2]
        if h == 'x': str += '0'
        else: str += h
    return str

def low_nybble_stream(bytes):
    str=""
    for b in bytes:
        str += hex(ord(b))[-1]
    return str

def reset_lowest_bit(bits):
    return bits & (bits -1)

def get_popcount(bits):
    count = 0
    while bits:
        bits = reset_lowest_bit(bits)
        count += 1
    return count

def serial_to_parallel(byte_stream, bit_streams):
    """
    We read a file from left to right. By analogy with how a number is laid out
    on paper, we read a file from "most significant position" to "least sig posn".
    The pablo functions are designed to start processing from the least sig position
    to the most sig position (right to left), though. This mismatch hurts my brain.

    We read the parallel bit streams from left to right, just like a file (since PBS
    are just a transposed view of the original file byte stream). We *build* PBS from
    right to left, though. That requires some minor logical acrobatics. We read the first
    byte from the input byte stream, decompose it and add into PBS, then shift each PBS left by
    one bit position before repeating the process with the next byte. Older bits appear farther
    left in the PBS. The net effect of this approach is that the final byte of the file appears
    as the final bit of the PBS.

    To makes things ever more ridiculous, we process each *byte* from right to left. Like this:
    11110101 & 1 = bit position 7.
    11110101 >> 1 = 01111010
    01111010 & 1 = bit position 6.
    ...
    """
    for byte in byte_stream:
        byte = ord(byte) # get integer representing code point
        bit_ordinality = 7 # 0 indexed
        #print(bin(byte))
        for i in range(8):
            #print("b4 ", bin(bit_streams[bit_ordinality]))
            bit_streams[bit_ordinality] <<= 1
            bit = (byte >> i) & 1
            bit_streams[bit_ordinality] |= bit
            #print("bit stream", bit_ordinality, bin(bit_streams[bit_ordinality]))
            bit_ordinality -= 1
    #print(bit_streams)

def inverse_transpose(bitset, len):
    """

    We read PBS like file, from left to right. Leftmost bit corresponds to
    first byte in file. However, PBS are integers and the easiest way to extract each
    bit is to AND each stream with 1 (i.e. at the least significant bit position).
    We use this "and with 1" approach, so we process
    the PBSs "backwards" (i.e. from the last byte in the file to the first). That's why
    we do bytestream = chr(byteval) + bytestream instead of bytestream += chr(byteval).
    """
    bytestream=""
    cursor = 1
    for i in range(0, len):
        byteval = 0
        for j in range(8):
            if bitset[j] & cursor != 0:
                # 0th pbs contains most sig bit
                byteval += 128 >> j # 128 = 10000000
        bytestream = chr(byteval) + bytestream
        cursor += cursor # *2, equiv to << 1
    return bytestream

def count_leading_ones(strm):
    ones = 0
    while (strm & 0xFFFFFFFF) == 0xFFFFFFFF:
        ones += 32
        strm >>= 32
    while (strm & 1) == 1:
        ones += 1
        strm >>= 1
    return ones

def set_lowest_bit(bits):
    return bits | (bits + 1)

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
        non_empty_pack = any(pack_mask & marker_stream)
        marker_stream >>= pack_size
        idx_marker_stream <<= 1
        idx_marker_stream = idx_marker_stream | non_empty_pack

    return idx_marker_stream

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
        leading_zeroes = count_leading_zeroes(pext_marker_stream)
        field = (1 << fw) - 1
        field <<= leading_zeroes
        field &= bit_stream
        field >>= leading_zeroes # field now contains extracted bit stream data

        # reset pext_marker_stream bits belonging to the field we just processed
        pext_marker_stream = pext_marker_stream & ~((1 << (leading_zeroes + fw)) - 1)
        extracted_bit_stream = (field << shift_amnt) | extracted_bit_stream
        shift_amnt += fw
    return extracted_bit_stream
#TODO get rid of field_widths, change extracted_bits_stream to source_bits_stream
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
        leading_zeroes = count_leading_zeroes(pdep_marker_stream)
        field = ((1 << fw) - 1)
        bp_bit_streams[bp_stream_idx] &= ~(field << leading_zeroes) # zero out deposit field
        field &= extracted_bits_stream
        extracted_bits_stream >>= fw
        bp_bit_streams[bp_stream_idx] |= (field << leading_zeroes)

        # reset pdep_marker_stream bits belonging to the field we just processed
        pdep_marker_stream = pdep_marker_stream & ~((1 << (leading_zeroes + fw)) - 1)

class BitStream:
    """Workaround to allow pass-by-value for ints."""
    def __init__(self, value):
        self.value = value
