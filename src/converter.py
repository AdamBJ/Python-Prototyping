""" Contains the abstract Converter class. Inherit from this class each time support
for a new target format is to be added to the transducer.
"""
import sys
import os
from abc import ABC, abstractmethod, abstractproperty

# workaround to get the import statements below working properly.
# see https://stackoverflow.com/questions/16981921/relative-imports-in-python-3
PACKAGE_PARENT = '..'
SCRIPT_DIR = os.path.dirname(os.path.realpath(os.path.join(os.getcwd(),
                                                           os.path.expanduser(__file__))))
sys.path.append(os.path.normpath(os.path.join(SCRIPT_DIR, PACKAGE_PARENT)))

from src.transducer_target_enums import TransductionTarget
from src import pablo
from src.field_width import calculate_field_widths

class Converter(ABC):
    """Base class that is subclassed to support a particular output format (e.g. JSON).

    This class defines the member variables and methods used to transduce a set of fields
    to a particular format. Concrete subclasses implement the required abstract methods
    and populate the required abstract properties in order to support transduction to
    a particular format.
    """
    @abstractproperty
    def field_widths(self):
        """Adds abstract member variable "field_widths" that concrete subclasses must define."""
        pass
    @abstractproperty
    def num_fields_per_unit(self):
        """Adds abstract member variable "num_fields_per_unit" that concrete subclasses must define."""
        pass

    @abstractmethod
    def transduce_field(self, field_wrapper, field_type, starts_or_ends_file):
        """Implementation is output format dependant. Any concrete subclasses of Converter
        must implement this method."""
        pass

    @abstractmethod
    def create_bpb_stream(self):
        """Implementation is output format dependant. Any concrete subclasses of Converter
        must implement this method."""
        pass

    @abstractmethod
    def verify_user_inputs(self, pack_size, byte_stream):
        """Implementation is output format dependant. Any concrete subclasses of Converter
        must implement this method."""
        pass
        
    def create_pdep_stream(self):
        """Generate a bit mask stream for use with the PDEP operation.

        Takes a list containing field widths and a target format and produces a PDEP
        marker stream. The PDEP marker stream shows where in an output stream the extracted
        bits should be inserted in order to complete the desired transduction operation.

        All streams are represented as (unbounded) integers. BitStream class is provided as a
        means of passing these integers to fxns "by reference". Doing so allows us to make changes
        to our "stream" variables inside methods and have these changes persist once the
        methods return.

        Returns (BitStream):
            The pdep bit stream.

        Examples:
            See test_pdep_stream_gen.py
        """
        pdep_marker_stream = pablo.BitStream(0)
        field_type = 0
        # Tracks number bits already written. We skip over these before inserting new transduced field
        shift_amnt = 0
        # process fields in the order they appear in the file, i.e. from left to right
        for i, field_width in enumerate(self.field_widths):
            starts_or_ends_file = True if i == 0 or i == (len(self.field_widths) - 1) else False
            field_wrapper = pablo.BitStream((1 << field_width) - 1) # create field
            num_boilerplate_bytes_added = self.transduce_field(field_wrapper, field_type,
                                                               starts_or_ends_file)
            self.insert_field(field_wrapper, pdep_marker_stream, shift_amnt)
            shift_amnt += num_boilerplate_bytes_added + field_width
            field_type += 1
            if field_type == self.num_fields_per_unit:
                field_type = 0
            #print(bin(pdep_marker_stream.value)) debug
        #print(bin(pdep_marker_stream.value)) #debug
        return pdep_marker_stream.value

    def insert_field(self, field_wrapper, pdep_marker_stream, shift_amount):
        """OR padded value into PDEP marker stream.

        Stream grows from right to left. First field we process (which is the first
        field that appear in the input file) is the left-most field represented by
        pdep_marker_stream. Last field we process (last field in the file) is the
        rightmost.
        """
        pdep_marker_stream.value = (field_wrapper.value << shift_amount) \
                                | pdep_marker_stream.value

    def verify_pack_size(self, pack_size):
        """Verify inputs provided by user."""
        if pack_size == 0 or (pack_size & (pack_size - 1)) != 0:
            # Credit to A.Polino for this check
            raise ValueError("Pack size must be a power of two.")

