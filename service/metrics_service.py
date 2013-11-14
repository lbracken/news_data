# -*- coding: utf-8 -*-
"""
    news_data.service.metrics_service
    ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    Module that provides metric data from the DB.

    :license: MIT, see LICENSE for more details.
"""

from datetime import datetime
from datetime import timedelta

from db import mongo
import date_util

db_metric_data_daily = None
db_metric_data_monthly = None


def init_db():
    global db_metric_data_daily, db_metric_data_monthly

    # Init DB
    db_metric_data_daily = mongo.get_metric_data_daily()
    db_metric_data_monthly = mongo.get_metric_data_monthly()


def get_term_counts(terms, time_start, time_end, verbose=False):
    """ Get counts of each the given set of terms.
        Returns an array of term data objects, and the granularity

    """
    term_data = []
    granularity = determine_granularity(time_start, time_end, verbose)

    for term in terms:

        if granularity == "DAILY":
            data, total, avg, max_val = get_daily_term_data(
                    term, time_start, time_end, granularity)

        elif granularity == "MONTHLY":
            data, total, avg, max_val = get_monthly_term_data(
                    term, time_start, time_end, granularity)

        term_data.append({
            "term"  : term,
            "data"  : data,
            "total" : total,
            "avg"   : avg,
            "max"   : max_val})

    return term_data, granularity


def determine_granularity(time_start, time_end, verbose):
    """ Determine the time granularity for the given time range

    """
    granularity = "MONTHLY"
    duration = time_end - time_start

    if duration <= timedelta(days=366):
        granularity = "DAILY"

    if verbose:
        print "Using time granularity of [%s]" % granularity

    return granularity


def get_daily_term_data(term, time_start, time_end, granularity):
    """ Get 'daily' granularity metric data for the given term and
        time range

    """
    data = []
    avg = 0 
    total = 0
    max_val = 0 
    data_pts = 0

    # Query the DB for all documents containing the
    # given term and within the given date range.
    db_query =   {"term" : term,
        "date" : {"$gte" : time_start},
        "date" : {"$lte" : time_end}}

    result_set = []
    result_set_idx = 0;
    for result in db_metric_data_daily.find(db_query).sort("date"):
        result_set.append(result)

    # Iterate over each month in the requested time range
    curr_month = time_start
    while (curr_month < time_end):

        # Typically the end day is the number of days in the month.
        # However, if this is the final month in the requested time
        # range, the day may be earlier based upon the end date.
        days_in_month = date_util.get_days_in_month(
                curr_month.year, curr_month.month)

        if date_util.is_same_month(curr_month, time_end):
            days_in_month = time_end.day

        data_pts += days_in_month

        # Get the next result from the result set (assume idx is in
        # bounds). If it matches the current month, then use its data
        # to build the response.  Otherwise, we have a gap in the data
        # which should be filled with an empty result.
        result = None
        if result_set_idx < len(result_set):
            result = result_set[result_set_idx]

        if result and date_util.is_same_month(curr_month, result["date"]):
            # Increment result_set_idx, if not 
            # already at the end of the list
            result_set_idx += 1 if result_set_idx < len(result_set) - 1 else 0
            for day in range(curr_month.day, days_in_month+1):
                val = result["daily"][str(day)]
                data.append(val)
                total += val
                if (val > max_val):
                    max_val = val
        else:
            for day in range(curr_month.day, days_in_month + 1):
                data.append(0)

        curr_month = date_util.get_next_month(curr_month)

    # Calculate the final average
    if data_pts > 0:
        avg = total / data_pts

    return data, total, avg, max_val


def get_monthly_term_data(term, time_start, time_end, granularity):
    """ Get 'month' granularity metric data for the given term and
        time range

    """
    data = []
    avg = 0 
    total = 0
    max_val = 0 
    data_pts = 0

    # Query the DB for the document containing monthly metrics
    db_query = {"term" : term}
    result = db_metric_data_monthly.find_one(db_query)

    # Iterate over each month in the requested time range
    curr_month = time_start
    while curr_month < time_end:

        if result == None:
            val = 1
        else:
            val = result[str(curr_month.year)][str(curr_month.month)]
        
        data_pts += 1
        data.append(val)
        total += val
        if val > max_val:
            max_val = val
        
        curr_month = date_util.get_next_month(curr_month)

    # Calculate the final average
    if data_pts > 0:
        avg = total / data_pts

    return data, total, avg, max_val


# Initialize connection to DB when loading module
init_db()