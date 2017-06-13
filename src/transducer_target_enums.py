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
    