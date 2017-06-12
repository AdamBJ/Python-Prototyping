# Python-Prototyping
Proof-of-concept programs written in Python that demonstrate the feasibility of various experimental Parabix approaches.

Bit streams grow from right to left, grows from position 0, the least significant position in the bit stream. Reasons for this:
1) Let least sig bit be RHB, most sig be LHB. Matches how we write number on paper with more sig column the leftmost column.
2) By numbering bits starting from RHB, the value of a bit stream can easily be computed as the sum of b(i) * 2^i, for i = 0 to i = n - 1.
3) Parabix requires this orientation to make carries work. Consider abcdefg. 1:1 mapping to bit stream would be gfedcba.
If we process bit stream using additions, bits get carried from right to left, which means they get carried *forward* in the
file. 

For more on bit numbering, see https://en.wikipedia.org/wiki/Bit_numbering
