def get_deletes_name(name: str) -> str:
    """
    Single method for naming resource deletes name.
    :param name:
    :return:
    """
    if name.endswith('_deletes'):
        return name
    else:
        return name + '_deletes'