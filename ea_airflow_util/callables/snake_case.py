import re

def snake_case(string: str) -> str:
    """
    Convert a string to snake_case and return.
    Inspired by `https://stackoverflow.com/a/1176023`.
    """
    string = re.sub('(.)([A-Z][a-z]+)' , r'\1_\2', string)  # Insert underscores to separate fully capitalized words.
    string = re.sub('([a-z0-9])([A-Z])', r'\1_\2', string)  # Insert underscores between all other words.
    string = re.sub('[ _-]+', '_', string) # Replace multiple-word identifiers with single underscores.
    return string.lower()

def record_to_snake_case(record: dict) -> dict:
    """
    Convert the keys of a row record to snake_case.
    Raise a KeyError if two keys collide after snake_casing.
    """
    snake_cased_record = {
        snake_case(str(key)): value for key, value in record.items()
    }

    # Raise an error if two keys coalesce into a single field.
    if len(snake_cased_record) != len(record):
        raise KeyError(
            "Failed conversion of record to snake_case! At least two fields coalesced!"
        )

    return snake_cased_record
