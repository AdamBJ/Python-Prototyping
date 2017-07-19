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
# Additions and refinements by Adam Bolding-Jones, June 2017
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

def match(s,marker):
    pos = count_forward_zeroes(marker)
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

def count_forward_zeroes(strm):
    """Count zeroes starting from position 0 bit in stream.

    Parabix uses LSB numbering, (see https://en.wikipedia.org/wiki/Bit_numbering)
    so for us the position 0 bit is the rightmost bit.
    Another way to think of this oepration is to count zeroes in the direction carry bits move.
    """
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

def print_aligned_u8_unicode_strings(u8_unicode_string):
    """Print out a set of 'encoding' aligned streams."""

    # Set the system info to print utf-8
    info = codecs.lookup('utf-8')
    sys.stdout = info.streamwriter(sys.stdout)

    label_max = max([len(p[0]) for p in u8_unicode_string])
    for p in u8_unicode_string:
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

# ---------------Functions below this line have been tested with Python 3.x only!!------------------

def serial_to_parallel(unicode_string, bit_streams):
    """Encode unicode_string as a utf-8 bytestream, then decompose the stream.

    We must encode the unicode string as a UTF-8 byte stream so that we have a concrete sequence of
    bytes to decompose into PBS. If the string is represented as a sequence of Unicode characters
    independent of a particular encoding we can't decompose it into parallel bit streams properly.
    Decomposing into UTF-8 as opposed to ASCII ensures that we can handle non-ascii characters.

    We follow a little-endian bit position numbering scheme. The least significant bit of each byte
    is bit 0, the  most significant bit is bit 7.

    Args:
        unicode_string (str): Unicode string representing the text we want to decompose into
            parallel bit streams. Unicode is the default str type in Python 3.x, not 2.x.
        bit_streams (list of int): Structure that will contain the eight parallel bit streams that
            result from decomposition.

    """
    byte_count = 0
    utf8_byte_string = unicode_string.encode() # Go from Unicode codepoints to UTF-8 byte stream
    # Decompose each byte in the string
    for byte in utf8_byte_string:
        for i in range(8):
            bit = (byte >> i) & 1
            bit_streams[i] = bit_streams[i] | (bit << byte_count)
        byte_count += 1

def inverse_transpose(bitset, len):
    """Construct output bytestream by reassembling the PBS into a byte stream.

    Bytestream grows left to right (in reading order), but we process bitsets
    from right to left as per
    bit stream processing convention. 
    
    This method creates bytes sequentially
    by combining the bits in bitset.

    Args:
        bitset: The set of bit streams to combine into a byte stream.
        len: The length of the bit streams.

    Returns:
        bytestream (str): The byte stream that results from combining the bit streams
        in bitset.

    Example:
    Bit streams:
    bs2: 101
    bs1: 111
    bs0: 100

    First we process the least sig column to get 110, then we move cursor over by one
    position and create 010, and finally 111.
    """
    bytestream = bytearray()
    cursor = 1
    for i in range(0, len):
        byteval = 0
        for j in range(8):
            if bitset[j] & cursor != 0: #cursor moves us along in the bit stream
                # 0th pbs contains least sig bit
                byteval += 1 << j
        bytestream.append(byteval)
        cursor += cursor # *2, equiv to << 1. Move to next bit position
    return bytestream.decode('utf-8')

def count_forward_ones(strm):
    """Count the number of consequtive 1s starting from the least sig bit position."""
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
    stream to identify packs of interest. Remember bit streams like idx_marker_stream grow R to L.
    See https://github.com/AdamBJ/Python-Prototyping/wiki/Bit-stream-growth-and-processing-order.

    Args:
        marker_stream (int): The stream to scan through. It's a bit stream, so process it
            from right to left (starting at pos 0, the least sig position).
        pack_size (int): The final index stream is marker_stream length / pack_size
            bits long. Each bit of the index stream represents a pack_sized "pack"
            of input stream bits. If the index stream bit is set, that means the corresponding
            pack in marker_stream contains at least one set bit and requires processing.
    Returns:
        idx_marker_stream (int): See pack_size comment.
    """
    idx_marker_stream = 0
    pack_mask = (1 << pack_size) - 1
    shift_amnt = 0
    while marker_stream:
        non_empty_pack = any(pack_mask & marker_stream) # set if pack contains fields
        idx_marker_stream = idx_marker_stream | (non_empty_pack << shift_amnt) # add bit to idx ms
        shift_amnt += 1
        marker_stream >>= pack_size

    return idx_marker_stream

def get_width_next_field(bit_stream):
    """ Return the width of the next field (sequence of 1s) in bit_stream.
    Example:
        bit_stream = 1110111110000
        field_width = 5
    """
    leading_zeroes = count_forward_zeroes(bit_stream)
    bit_stream >>= leading_zeroes
    fw = count_forward_ones(bit_stream)
    return fw

def create_pext_ms(byte_stream, target_characters, get_inverse=False):
    """Create a PEXT marker stream based on characters in target_characters.
    
    Scans through byte_stream, adds single bit to pext_ms for each byte in byte_
    stream. If a character in byte_stream matches a character in target_characters
    && get_inverse == False, we add a 1 bit to the pext_ms. Otherwise we add a 0. 

    Example:
        byte_stream = abc,123\n
        target_characters = [",", "\n"]
        get_inverse = False

        pext_marker_stream - 1110111

    Remember to read pext_marker_stream from right to left.
    """
    pext_marker_stream = 0
    shift_amnt = 0
    for character in byte_stream:
        condition = None
        if get_inverse:
            condition = character not in target_characters
        else:
            condition = character in target_characters

        if condition:
            num_bytes = len(character.encode())
            char_mask = int(('1' * num_bytes), 2)
            pext_marker_stream = (char_mask << shift_amnt) | pext_marker_stream
        else:
            num_bytes = len(character.encode())
        shift_amnt += num_bytes

    return pext_marker_stream

def apply_pext(bit_stream, pext_marker_stream):
    """Apply quick-and-dirty python version of PEXT to bit_stream.

    Args:
        bit_stream: The bit stream we'll extract bits from.
        pext_marker_stream: Marker stream that tells us which bit positions in bit_stream
            to extract bits from.
    Returns:
        extracted_bit_stream: The bit stream that results from extracting bits from bit_stream
            at the locations indicated by pext_marker_stream.
    Example:
        bit_stream:           1010001110
        pext_marker_stream:   1111110001

        extracted_bit_stream: 101000   0
    """
    extracted_bit_stream = 0
    shift_amnt = 0
    while pext_marker_stream:
        leading_zeroes = count_forward_zeroes(pext_marker_stream)
        fw = get_width_next_field(pext_marker_stream)
        field = (1 << fw) - 1
        field <<= leading_zeroes
        field &= bit_stream
        field >>= leading_zeroes # field now contains extracted bit stream data

        # Reset pext_marker_stream bits belonging to the field we just processed
        pext_marker_stream = pext_marker_stream & ~((1 << (leading_zeroes + fw)) - 1)
        # Insert the bits we extracted into extracted_bit_stream
        extracted_bit_stream = (field << shift_amnt) | extracted_bit_stream
        shift_amnt += fw
    return extracted_bit_stream

def apply_pdep(bp_bit_streams, bp_stream_idx, pdep_marker_stream, source_bit_stream):
    """Apply quick-and-dirty Python version of pdep to bp_bit_streams[bp_stream_idx].

    Process the stream from least significant bit (right) to most significant bit (left).

    Args:
        bp_bit_streams: Stream set that contains the stream the pdep operation will be applied to.
        bp_stream_idx: Index in bp_bit_streams of the stream we will apply the pdep operation to.
        pdep_marker_stream: Marker stream that tells us the positions within bp_bit_streams[bp_stream_idx]
            that the extracted bits should be deposited.
        source_bit_stream: The stream we will be inserting into bp_bit_streams[bp_stream_idx] at the
            locations indicated by pdep_marker_stream. Consists of extracted field bits, e.g. for a CSV file
            source_bit_stream could be obtained by applying s2p on a version of the CSV file with the
            delimiters stripped out (we use a PEXT operation, though). Remeber that source
            bit_stream is a bit stream, not a byte stream. We need to combine 8 source_bit_streams using
            p2s to obtain the fields byte stream.
    Example:
        source_bit_stream =                                    101011
        pdep_marker_stream =           000000001110000000011100000000
        bp_bit_stream[bp_stream_idx] = 000000000000000000000000000000

        bp_bit_stream[bp_stream_idx] = 000000001010000000001100000000
    """
    while pdep_marker_stream:
        leading_zeroes = count_forward_zeroes(pdep_marker_stream)
        fw = get_width_next_field(pdep_marker_stream)
        field = ((1 << fw) - 1)
        bp_bit_streams[bp_stream_idx] &= ~(field << leading_zeroes) # zero out deposit field
        field &= source_bit_stream
        source_bit_stream >>= fw
        bp_bit_streams[bp_stream_idx] |= (field << leading_zeroes)

        # reset pdep_marker_stream bits belonging to the field we just processed
        pdep_marker_stream = pdep_marker_stream & ~((1 << (leading_zeroes + fw)) - 1)

class BitStream:
    """Workaround to allow pass-by-value for ints."""
    def __init__(self, value):
        self.value = value
