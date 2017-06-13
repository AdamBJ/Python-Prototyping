"""
Contains entry for each target format we support transduction to.
Knowing the target transduction format allows us to calulate the amount
of boilerplate bytes that need to be added to each source field to
transduce it.

For example,
the amount of JSON boilerplate required to transduce an extracted field to JSON
depends on it's ordinality within the JSON object that will contain it (e.g. the
amount of boilerplate required for the first field differs from the amount for
the second field) as well as the length of the column names in the CSV file
(which is passed in as input). As long as the ordinality and column name info
is available at runtime, we can dynamically determine how much padding an
extracted field requires.
"""

from enum import Enum

class TransductionTarget(Enum):
    """Enumerates the target transduction formats we support."""
    JSON = 1
    CSV = 2

    def create_bpb_stream(self, field_widths, num_fields_per_unit, csv_column_names):
        """Create boilerplate byte stream.

        The boilerplate byte stream is a stream of boilerplate characters with
        space added for values extracted from the input file (e.g. CSV values).
        The input file values will be inserted into the stream later by PDEP operations.
        Currently uses quick-and-dirty concatenation (+=). This may change in the Parabix version.

        field_type tracks where we are in the current source format object we're creating.
        For example, for JSON, field_type tracks how many fields we've added to the object.
        We use this information to determine which boilerplate bytes we should add next.

        Args:
            num_fields_per_unit: The number of fields in a "unit" of output. For example,
            output units for JSON are JSON objects that contain num_fields_per_unit fields.
            For CSV files, num_fields_per_unit is just one, since a CSV unit is whatever's
            between two delimiters.

        Returns:
            The boilerplate byte stream (str).

        Example:
            For a CSV input file with a single value that's three characters wide,
            return [\n    {\n        "columnName": ___\n        }\n].
        """
        preceeding_boilerplate_bytes = 0
        following_boilerplate_bytes = 0
        ff_pbb = 0 
        ff_fbb = 0
        lf_pbb = 0
        lf_fbb = 0

        if self == TransductionTarget.JSON:
            # TODO prompt for column names
            # num_fields_per_unit == number CSV values per row in CSV file
            if len(field_widths) % num_fields_per_unit != 0:
                raise ValueError("Provided source fields cannot be cleanly packaged into JSON objects.")

            field_type = 0
            num_json_objects_to_create = len(field_widths) / num_fields_per_unit
            num_json_objs_created = 0
            json_bp_byte_stream = "[\n"
            for fw in field_widths:
                if field_type == 0:
                    # first field in JSON object, start new JSON object
                    json_bp_byte_stream += "    {\n"

                # Add key/value pair. Indent key value pairs within {} and objects within []
                json_bp_byte_stream += "        "
                json_bp_byte_stream += "\"" + csv_column_names[field_type] + "\": "
                json_bp_byte_stream += "_" * fw  # space for value

                if field_type == (num_fields_per_unit - 1):
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

        else:
            raise ValueError(
                "Only CSV to JSON transduction is currently supported.")

    # TODO check handles non-ASCII encodings (everything but UTF-16 should work)
    # TODO remove csv_column_names as argument. Only needed if target is CSV. Parse/prompt
    # user if required
    def transduce_field(self, field_wrapper, field_type, csv_column_names):
        """ Pad extracted field with appropriate boilerplate.

        The amount of boilerplate padding we need to add depends on how many
        boilerplate bytes were used to create the boilerplate byte stream in create_bpb_stream.

        Args:
            field_wrapper (BitStream): The field to transduce.
            field_type: A scalar describing the type of the field to be transduced (i.e.
                it's ordinality within the data unit it will belong to in the output).
            csv_column_names: The names of the columns in the input CSV file.
        Returns:
            Number of boilerplate padding bytes added. Also, field_wrapper is "passed by
            reference", so the changes we make to field_wrapper.value persist after
            this function returns.
        Example:
            >>>transduce_field(111, 0, ["col1"])
            10
            Note: original field transformed from 111 to (00)11100000000000000000000000

            23 preceeding boilerplate bits:
                6 from  `    {\n`
                13 from `         "": `
                4 from  `col1`

            2 following boilerplate bits:
                2 from   `,\n`

            Together: `    {\n        "col1": ___,\n

        """
        preceeding_boilerplate_bytes = 0
        following_boilerplate_bytes = 0
        if self == TransductionTarget.JSON:
            #TODO "Please enter column names / parse column names from file"
            #           "<col_name>": 
            preceeding_boilerplate_bytes = 12 + len(csv_column_names[field_type].encode('utf-8'))
            #,\n  or \n} or \n] TODO quotes around value?
            following_boilerplate_bytes = 2
            if field_type == 0:
                preceeding_boilerplate_bytes += 6 #    {\n
            elif field_type == len(csv_column_names) - 1:
                following_boilerplate_bytes += 6 #    \n}
        else:
            raise ValueError("Only JSON is currently supported as a target language.")

        field_wrapper.value = field_wrapper.value << preceeding_boilerplate_bytes
        return preceeding_boilerplate_bytes + following_boilerplate_bytes
