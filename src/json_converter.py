"""
Contains JSONConverter, a class that contains key methods and data required
transduce a file to JSON.
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
from src.converter import Converter
from src import pablo

class JSONConverter(Converter):
    """Contains data and methods used to convert a set of extracted fields to JSON format.
    """
    def __init__(self, field_widths, json_object_field_names):
        self._json_object_field_names = json_object_field_names
        self._num_fields_per_unit = len(json_object_field_names)
        self._field_widths = field_widths

    # Boilerplate for abstract attribute implementation. Read-only.
    @property
    def num_fields_per_unit(self):
        return self._num_fields_per_unit

    @property
    def field_widths(self):
        return self._field_widths

    def verify_user_inputs(self, pack_size, byte_stream):
        """Ensure that the user has provided a valid pack size
        and that the input file they've provided contains valid
        data."""
        self.verify_pack_size(pack_size)
        self.verify_byte_stream(byte_stream)

    def verify_byte_stream(self, byte_stream):
        """Check that each row of the input file is well formed."""
        field_end_ms = pablo.create_pext_ms(byte_stream, [",", "\n"])
        csv_bit_streams = [0, 0, 0, 0, 0, 0, 0, 0]
        extracted_bit_streams = [0, 0, 0, 0, 0, 0, 0, 0]
        pablo.serial_to_parallel(byte_stream, csv_bit_streams)
        for i in range(8):
            extracted_bit_streams[i] = pablo.apply_pext(csv_bit_streams[i], field_end_ms)
        extracted_delim_stream = pablo.inverse_transpose(extracted_bit_streams,
                                                         pablo.get_popcount(field_end_ms))

        count = 0
        for delimiter in extracted_delim_stream:
            if count == (self._num_fields_per_unit - 1) and delimiter != "\n":
                raise ValueError("Input CSV file contains row missing a newline terminator.")
            elif count == (self._num_fields_per_unit - 1): # found the newline
                count = 0
            else:
                count += 1

        # Check for incomplete rows
        if count != 0:
            raise ValueError("Input CSV file contains malformed row.")

    def create_bpb_stream(self):
        """Create boilerplate byte stream.

        The boilerplate byte stream is a stream of boilerplate characters with
        space added for values extracted from the input file (e.g. CSV values).
        The input file values will be inserted into the stream later, at the empty
        positions, by PDEP operations. Currently uses quick-and-dirty concatenation (+=).

        field_type tracks where we are in the current target format object we're creating.
        For example, for JSON, field_type tracks how many fields we've added to the object.
        We use this information to determine which boilerplate bytes should be added at each
        step.

        Example:
            For a CSV input file with a single value that's three characters wide,
            return [\n    {\n        "columnName": ___\n        }\n].
        """
        # self.num_fields_per_unit == number CSV values per row in CSV file
        if len(self.field_widths) % self.num_fields_per_unit != 0:
            raise ValueError("Provided source fields cannot be cleanly packaged into JSON objects.")

        field_type = 0
        num_json_objects_to_create = len(self.field_widths) / self.num_fields_per_unit
        num_json_objs_created = 0
        json_bp_byte_stream = "[\n"
        for fw in self.field_widths:
            if field_type == 0:
                # first field in JSON object, start new JSON object
                json_bp_byte_stream += "    {\n"

            # Add key/value pair. Indent key value pairs within {} and objects within []
            json_bp_byte_stream += "        "
            json_bp_byte_stream += "\"" + self._json_object_field_names[field_type] + "\": "
            json_bp_byte_stream += "_" * fw  # space for value

            if field_type == (self.num_fields_per_unit - 1):
                num_json_objs_created += 1
                # final field, close JSON object
                json_bp_byte_stream += "\n    }"
                if num_json_objs_created == num_json_objects_to_create:
                    json_bp_byte_stream += "\n]"
                else:
                    json_bp_byte_stream += ",\n"
                field_type = 0  # reset
            else:
                # first or middle field
                json_bp_byte_stream += ",\n"
                field_type += 1

        return json_bp_byte_stream

    def transduce_field(self, field_wrapper, field_type, starts_or_ends_file):
        """Pad extracted field with appropriate JSON boilerplate.

        The amount of boilerplate padding we need to add depends on how many
        boilerplate bytes were used to create the boilerplate byte stream in create_bpb_stream.

        Args:
            field_wrapper (BitStream): The field to transduce.
            field_type: A scalar describing the type of the field to be transduced (i.e.
                it's ordinality within the data unit it will belong to in the output).
            starts_or_ends_file (boolean): True if this is the first field we've processed
                or the last field we will process in a file. Tells us when to add special
                starting/ending boilerplate syntax.
        Returns:
            Number of boilerplate padding bytes added. Also, field_wrapper is "passed by
            reference", so the changes we make to field_wrapper.value persist after
            this function returns.
        Example:
            See test_csv_json_transducer.py

        """
        preceeding_boilerplate_bytes, following_boilerplate_bytes = \
            self.get_preceeding_following_bpb(field_type, starts_or_ends_file)
        field_wrapper.value = field_wrapper.value << preceeding_boilerplate_bytes
        return preceeding_boilerplate_bytes + following_boilerplate_bytes

    def get_preceeding_following_bpb(self, field_type, starts_or_ends_file):
        """Get number boilerplate bytes following and preceeding the current field."""
        preceeding_boilerplate_bytes = 0
        following_boilerplate_bytes = 0
        #           "<col_name>": 
        # Encode as UTF-8 byte stream to handle Unicode characters in column names.
        preceeding_boilerplate_bytes = 12 + len(self._json_object_field_names[field_type].encode('utf-8'))
        #,\n  or \n} or \n] TODO quotes around value?
        following_boilerplate_bytes = 2
        if field_type == 0:
            if starts_or_ends_file:
                # [\n
                preceeding_boilerplate_bytes += 2
            preceeding_boilerplate_bytes += 6 #    {\n
        elif field_type == self._num_fields_per_unit - 1:
            following_boilerplate_bytes += 6  #    \n}

        return (preceeding_boilerplate_bytes, following_boilerplate_bytes)

    def transduce(self, file_as_str, fields_pext_ms, return_extracted_bs=False):
        """Transduce file_as_str to JSON.

        Args:
            file_as_str (str): The input file. Remember that in Python 3.x a str is a *Unicode*
                str. It stores a sequence of Unicode codepoints.  Encode to a particular format
                if you want to get a byte stream in a particular encoding.
            fields_pext_ms: A marker stream that shows where in file_as_str the fields we want to
                extract lie. A set bit in field_pext_ms corresponds to a byte we want to extract.
            return_extracted_bs: A flag that can be enabled for debugging purposes if the user wants
                to see what fields were extracted from the file.

        Returns:
            output_byte_stream (str): A string represented output JSON.
        """
        pdep_marker_stream = self.create_pdep_stream()
        json_bp_byte_stream = self.create_bpb_stream()
        json_bp_bit_streams = [0, 0, 0, 0, 0, 0, 0, 0]
        csv_bit_streams = [0, 0, 0, 0, 0, 0, 0, 0]
        extracted_bit_streams = [0, 0, 0, 0, 0, 0, 0, 0]
        # Decompose the input bytestream and output byte stream template into parallel bit streams
        pablo.serial_to_parallel(file_as_str, csv_bit_streams)
        pablo.serial_to_parallel(json_bp_byte_stream, json_bp_bit_streams)

        # Transduce
        for i in range(8):
            # Extract bits from CSV bit streams and deposit in bp bit streams.
            extracted_bit_streams[i] = pablo.apply_pext(csv_bit_streams[i], fields_pext_ms)
            pablo.apply_pdep(json_bp_bit_streams, i, pdep_marker_stream, extracted_bit_streams[i])

        # Combine the transduced parallel bit streams into the final output byte stream
        output_byte_stream = pablo.inverse_transpose(json_bp_bit_streams, len(json_bp_byte_stream)) # Unicode str in Python 3.x
        extracted_byte_stream = pablo.inverse_transpose(extracted_bit_streams, pablo.get_popcount(fields_pext_ms))
        if return_extracted_bs:
            return output_byte_stream, extracted_bit_streams
        else:
            return output_byte_stream
