# -*- coding: utf-8 -*-
"""
    news_data.service.stats_service
    ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    Module that provides statistics about data in the DB.  Useful to
    see what's loaded in the DB, and to monitor + debug the processing
    pipeline.

    :license: MIT, see LICENSE for more details.
"""

from db import mongo


db_raw_articles = None
db_parsed_articles = None
db_analyzed_articles = None
get_metric_data_daily = None
get_metric_data_monthly = None


def init_db():
    global db_raw_articles, db_parsed_articles, db_analyzed_articles,\
            get_metric_data_daily, get_metric_data_monthly

    # Init DB
    db_raw_articles = mongo.get_raw_articles()
    db_parsed_articles = mongo.get_parsed_articles()
    db_analyzed_articles = mongo.get_analyzed_articles()
    db_metric_data_daily = mongo.get_metric_data_daily()
    db_metric_data_monthly = mongo.get_metric_data_monthly()


def get_raw_articles_stats(time_start, time_end):
    """ Get stats about raw_articles from mongoDB

    """
    time_bound_query = create_time_bound_query(time_start, time_end)
    response = {"count" : []}
    for count in db_raw_articles.find(time_bound_query).sort("published"):
        response["count"].append(count["count"])

    return response


def get_parsed_articles_stats(time_start, time_end):
    return None


def get_analyzed_articles_stats(time_start, time_end):
    return None

def get_metric_data_daily_stats(time_start, time_end):
    return None

def get_metric_data_monthly_stats(time_start, time_end):
    return None


def create_time_bound_query(time_start, time_end):
    return {
        "published" : {"$gte" : time_start},
        "published" : {"$lte" : time_end}
    }


# Initialize connection to DB when loading module
init_db()