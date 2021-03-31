def require_at_least_one_record(cls, values: dict) -> dict:
    """Validates, that there is at least one record provided.

    This function exists, because pydantic.conlist doesnt support generics.
    """

    if not values['records']:
        raise ValueError('At least one record should be provided.')

    return values
