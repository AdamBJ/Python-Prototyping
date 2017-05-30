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
    global data
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
    data  = s
    return cursor-1  # basis streams and EOF mask

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
def EOF_mask(strm_lgth):
    mask = 1
    mask <<= (strm_lgth)
    mask -= 1
    return mask

def extract_bit(strm, pos):
    bit = (strm >> pos) & 1
    return bit

def inverse_transpose(bitset, len):
    bytestream=""
    cursor = 1
    for i in range(0, len):
        byteval = 0
        for j in range(0,8):
            if bitset[j] & cursor != 0:
                byteval += 128 >> j
        bytestream += chr(byteval)
        cursor += cursor
    return bytestream

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

class IntWrapper:
    """Workaround to allow pass-by-value for ints."""
    def __init__(self, value):
        self.value = value

