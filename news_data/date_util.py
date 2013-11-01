# -*- coding: utf-8 -*-
"""
    news_data.date_util
    ~~~~~~~~~~~~~~~~~~~~~~~~ 

    This module provides various utility functions for working with
    dates.

    :license: MIT, see LICENSE for more details.
"""

import calendar


def get_days_in_month(yyyy, mm):
    """ Returns the number of days in the given month

    """
    return calendar.monthrange(yyyy, mm)[1]


def pad_month_day_value(to_pad):
    ''' Pads the given month or day value with a preceding '0' if
        needed. For example, turns 2 into '02.  Returned result will
        always be a string
    '''
    return str(to_pad if to_pad > 9 else "0%d" % to_pad)