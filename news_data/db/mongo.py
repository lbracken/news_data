# -*- coding: utf-8 -*-
"""
    news_data.db.mongo
    ~~~~~~~~~~~~~~~~~~ 

    This module supports interaction with mongoDB.

    Usage: Call a get_<collection_name>() function to return a
    connection to a particular mongoDB collection.  The connection
    should he reused to limit the number of concurrent connections
    open to mongoDB.
    
    :license: MIT, see LICENSE for more details.
"""

import ConfigParser

import bson
import pymongo


def init_config():
    """ Read mongoDB connection settings from config file

    """
    global db_host, db_name

    config = ConfigParser.SafeConfigParser()
    config.read("../settings.cfg")

    db_host = config.get("mongo", "db_host")
    db_port = config.getint("mongo", "db_port")

    print 'Connecting to mongoDB @ %s:%d' % (db_host, db_port)  


def get_mongodb_connection():
    client = pymongo.MongoClient(db_host, db_port)
    db = client["newsDataDB"]
    return db


def get_raw_articles():
    collection = get_mongodb_connection()["rawArticles"]
    return collection


def get_parsed_articles():
    collection = get_mongodb_connection()["parsedArticles"]
    return collection


def get_analyzed_articles():
    collection = get_mongodb_connection()["analyzedArticles"]
    return collection


def get_metric_data_daily():
    collection = get_mongodb_connection()["metricDataDaily"]
    collection.ensure_index([('term', 1), ('date', 1)])
    return collection


def get_metric_data_monthly():    
    collection = get_mongodb_connection()["metricDataMonthly"]
    collection.ensure_index('term', 1)
    return collection


init_config()