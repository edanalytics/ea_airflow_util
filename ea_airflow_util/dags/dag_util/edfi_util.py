def get_deletes_name(name):
    """
    Single method for naming resource deletes name.
    :param name:
    :return:
    """
    if name.endswith('_deletes'):
        return name
    else:
        return name + '_deletes'