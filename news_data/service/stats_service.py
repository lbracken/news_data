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
db_metric_data_daily = None
db_metric_data_monthly = None


def init_db():
    global db_raw_articles, db_parsed_articles, db_analyzed_articles,\
            db_metric_data_daily, db_metric_data_monthly

    # Init DB
    db_raw_articles = mongo.get_raw_articles()
    db_parsed_articles = mongo.get_parsed_articles()
    db_analyzed_articles = mongo.get_analyzed_articles()
    db_metric_data_daily = mongo.get_metric_data_daily()
    db_metric_data_monthly = mongo.get_metric_data_monthly()


def get_collection_counts(time_start, time_end):
    """ Get counts of each collection.

    """
    time_bound_query = create_time_bound_query(time_start, time_end)
    response = {
        "total_parsed_articles" : 
                db_parsed_articles.find(time_bound_query).count(),
        "total_analyzed_articles" : 
                db_analyzed_articles.find(time_bound_query).count(),
        "total_metric_data_daily" : 
                db_metric_data_daily.count(),    #TODO: Limit to time range
        "total_metric_data_monthly" : 
                db_metric_data_monthly.count()   #TODO: Limit to time range
    }

    # Raw articles are a special case, requires an aggregation
    response["total_raw_articles"] = db_raw_articles.aggregate([
        {"$match" : time_bound_query},      
        {"$project" : {"count" : "$count"}},
        {"$group" : {"_id" : 1, "total_count" : {"$sum" : "$count"}}}
    ]).get("result")[0].get("total_count")

    return response

def get_raw_articles_stats(time_start, time_end):
    """ Get stats about raw_articles from mongoDB

    """
    time_bound_query = create_time_bound_query(time_start, time_end)
    response = {"count" : []}
    for count in db_raw_articles.find(time_bound_query).sort("published"):
        response["count"].append(count["count"])

    return response


def get_parsed_articles_stats(time_start, time_end):
    """ Get stats about parsed_articles from mongoDB

    """
    time_bound_query = create_time_bound_query(time_start, time_end)
    parsed_articles_stats = db_parsed_articles.aggregate([
        {"$match" : time_bound_query},
        {"$project" : {
            "date" : { 
                "y" : { "$year" : "$published" },
                "m" : { "$month" : "$published" }},
            "size_raw" : "$size_raw",
            "size_parsed" : "$size_parsed",
            "size_ratio"  : "$size_ratio"
        }},
        {"$group" : {
            "_id" : "$date",
            "count" : {"$sum" : 1},
            "size_raw_sum" : {"$sum" : "$size_raw"},
            "size_raw_avg" : {"$avg" : "$size_raw"},
            "size_raw_min" : {"$min" : "$size_raw"},
            "size_raw_max" : {"$max" : "$size_raw"},
            "size_parsed_sum" : {"$sum" : "$size_parsed"},
            "size_parsed_avg" : {"$avg" : "$size_parsed"},
            "size_parsed_min" : {"$min" : "$size_parsed"},
            "size_parsed_max" : {"$max" : "$size_parsed"},
            "size_ratio_avg" : {"$avg" : "$size_ratio"},
            "size_ratio_min" : {"$min" : "$size_ratio"},
            "size_ratio_max" : {"$max" : "$size_ratio"}
        }},
        {"$sort" : {"_id" : 1}}
    ])

    # Init the response object, then add each data point
    response = {
        "count" : [],
        "size_raw_sum" : [],
        "size_raw_avg" : [],
        "size_raw_min" : [],
        "size_raw_max" : [],
        "size_parsed_sum" : [],
        "size_parsed_avg" : [],
        "size_parsed_min" : [],
        "size_parsed_max" : [],
        "size_ratio_avg" : [],
        "size_ratio_min" : [],
        "size_ratio_max" : []
    }

    for dp in parsed_articles_stats["result"]:
        response["count"].append(dp["count"])
        response["size_raw_sum"].append(dp["size_raw_sum"])
        response["size_raw_avg"].append(dp["size_raw_avg"])
        response["size_raw_min"].append(dp["size_raw_min"])
        response["size_raw_max"].append(dp["size_raw_max"])
        response["size_parsed_sum"].append(dp["size_parsed_sum"])
        response["size_parsed_avg"].append(dp["size_parsed_avg"])
        response["size_parsed_min"].append(dp["size_parsed_min"])
        response["size_parsed_max"].append(dp["size_parsed_max"])
        response["size_ratio_avg"].append(dp["size_ratio_avg"])
        response["size_ratio_min"].append(dp["size_ratio_min"])
        response["size_ratio_max"].append(dp["size_ratio_max"])

    # To work around RaphaelJS Graph bug, show ratio
    # as 0-100 instead # of 0.0 to 1.0
    for i in range (len(response["size_ratio_avg"])):
        response["size_ratio_avg"][i] = int(response["size_ratio_avg"][i]*100)
        response["size_ratio_min"][i] = int(response["size_ratio_min"][i]*100)
        response["size_ratio_max"][i] = int(response["size_ratio_max"][i]*100)


    return response


def get_analyzed_articles_stats(time_start, time_end):
    """ Get stats about analyzed_articles from mongoDB

    """
    time_bound_query = create_time_bound_query(time_start, time_end)    
    analyzed_articles_stats = db_analyzed_articles.aggregate([
        {"$match" : time_bound_query},      
        {"$project" : {
            "date" : {
                "y" : { "$year" : "$published" },
                "m" : { "$month" : "$published" }},
            "unique_terms_count" : "$unique_terms_count",
            "total_terms_count"  : "$total_terms_count"
        }},
        {"$group" : {
            "_id" : "$date",
            "count" : {"$sum" : 1},
            "unique_terms_sum" : {"$sum" : "$unique_terms_count"},
            "unique_terms_avg" : {"$avg" : "$unique_terms_count"},
            "unique_terms_min" : {"$min" : "$unique_terms_count"},
            "unique_terms_max" : {"$max" : "$unique_terms_count"},
            "total_terms_sum" : {"$sum" : "$total_terms_count"},
            "total_terms_avg" : {"$avg" : "$total_terms_count"},
            "total_terms_min" : {"$min" : "$total_terms_count"},
            "total_terms_max" : {"$max" : "$total_terms_count"}
        }},
        {"$sort" : {"_id" : 1}}
    ])

    # Init the response object, then add each data point
    response = {
        "count" : [],
        "unique_terms_sum" : [],
        "unique_terms_avg" : [],
        "unique_terms_min" : [],
        "unique_terms_max" : [],
        "total_terms_sum" : [],
        "total_terms_avg" : [],
        "total_terms_min" : [],
        "total_terms_max" : []
    }

    for dp in analyzed_articles_stats["result"]:
        response["count"].append(dp["count"])
        response["unique_terms_sum"].append(dp["unique_terms_sum"])
        response["unique_terms_avg"].append(dp["unique_terms_avg"])
        response["unique_terms_min"].append(dp["unique_terms_min"])
        response["unique_terms_max"].append(dp["unique_terms_max"])
        response["total_terms_sum"].append(dp["total_terms_sum"])
        response["total_terms_avg"].append(dp["total_terms_avg"])
        response["total_terms_min"].append(dp["total_terms_min"])
        response["total_terms_max"].append(dp["total_terms_max"])

    return response


def get_metric_data_daily_stats(time_start, time_end):
    return {}


def get_metric_data_monthly_stats(time_start, time_end):
    return {}


def create_time_bound_query(time_start, time_end):
    return {
        "published" : {"$gte" : time_start},
        "published" : {"$lte" : time_end}
    }


# Initialize connection to DB when loading module
init_db()