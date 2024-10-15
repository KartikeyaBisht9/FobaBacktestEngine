from enum import Enum, EnumMeta

class MyRow:
    def __init__(self, row_dict):
        self._row_dict = row_dict
        self._add_attributes()

    def __getitem__(self, key):
        return self._row_dict[key]

    def __iter__(self):
        return iter(self._row_dict)

    def __len__(self):
        return len(self._row_dict)

    def keys(self):
        return self._row_dict.keys()

    def values(self):
        return self._row_dict.values()

    def items(self):
        return self._row_dict.items()

    def _add_attributes(self):
        for key in self._row_dict:
            setattr(self, key, self._row_dict[key])

def apply_mapping(df, column, mapping, final_column_name=None):
    """
    Apply mapping to a DataFrame column based on the provided mapping dictionary or Enum.

    Parameters:
        column (str): Name of the DataFrame column to be mapped.
        mapping (dict or Enum): Mapping dictionary or Enum to be applied.
        final_column_name (str, optional): Name of the resulting column. Defaults to None.

    Returns:
        pandas.Series: Series with mapped values.
    """
    if final_column_name is None:
        final_column_name = column

    # Define a function to apply mapping to a single value
    def map_value(value):
        if isinstance(mapping, Enum) or isinstance(mapping, EnumMeta):
            return mapping[value].value if value in mapping.__members__ else value
        elif isinstance(mapping, dict):
            return mapping.get(value, value)
        else:
            return value

    # Apply the mapping function to the DataFrame column
    return df[column].apply(map_value)
