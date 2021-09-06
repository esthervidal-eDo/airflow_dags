import collections
import re

from airflow import AirflowException


def remove_gs_from_bucket(bucket_name):
    return re.sub(pattern="^gs://", repl="", string=bucket_name)


# Recursive dictionary merge
# Copyright (C) 2016 Paul Durivage <pauldurivage+github@gmail.com>
def dict_merge(original_dct, merge_dct):
    """ Recursive dict merge. Inspired by :meth:``dict.update()``, instead of
    updating only top-level keys, dict_merge recurses down into dicts nested
    to an arbitrary depth, updating keys. The ``merge_dct`` is merged into
    ``dct``.

    :param copy_dct: dict onto which the merge is executed
    :param merge_dct: dct merged into dct
    :return: None
    """
    copy_dct = original_dct.copy()
    for k, v in merge_dct.items():
        if k in copy_dct:
            if isinstance(copy_dct[k], dict) and isinstance(merge_dct[k], dict):
                copy_dct[k] = dict_merge(copy_dct[k], merge_dct[k])
            elif isinstance(copy_dct[k], list) and isinstance(merge_dct[k], list):
                copy_dct[k].extend(merge_dct[k])
            elif (isinstance(copy_dct[k], dict) and isinstance(merge_dct[k], list)) \
                    or (isinstance(copy_dct[k], list) and isinstance(merge_dct[k], dict)):
                raise AirflowException(
                    "Failed when trying to mix key {key}, invalid types {type1} and {type2} for merging!" \
                        .format(key=k, type1=type(copy_dct[k]), type2=type(merge_dct[k])))
            else:
                copy_dct[k] = merge_dct[k]
        else:
            copy_dct[k] = merge_dct[k]
    return copy_dct


def iterate_nested_values(iterable, what_to_get=str):
    """Returns an iterator that returns all keys or values
       of a (nested) iterable.

       Arguments:
           - iterable: <list> or <dictionary>
           - returned: <string> "key" or "value"

       Returns:
           - <iterator>
    """
    if isinstance(iterable, dict):
        for key, value in iterable.items():
            if isinstance(value, what_to_get):
                yield value
            yield from iterate_nested_values(value, what_to_get=what_to_get)
    if isinstance(iterable, list):
        for el in iterable:
            if isinstance(el, what_to_get):
                yield el
            yield from iterate_nested_values(el, what_to_get=what_to_get)


def find_regex(document, regex):
    """
    Searches all values of a dictionary, including lists and nested dictionaries, and gets all matches of a regex
    :param dct:
    :param string:
    :return:
    """
    return [a for m in [re.findall(regex, l) for l in iterate_nested_values(document, what_to_get=str)] if m for a in m]
