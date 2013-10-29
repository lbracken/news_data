# -*- coding: utf-8 -*-
"""
    news_data.pipeline.metric_writer
    ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    [Pipeline Step 4 of 4]   

    This module subscribes to the 'analyzed_articles' queue and listens
    for jobs. When it receives a job, it reads the analyzed article
    results and creates metric data.  This is then written to the DB.

    :license: MIT, see LICENSE for more details.
"""

import argparse
from datetime import datetime

from db import mongo
import queue


verbose = False
updt_freq = 1000

metricized_articles = 0
connection = None
consume_channel = None
db_analyzed_articles = None
db_metric_data_daily = None
db_metric_data_monthly = None


def init_db_and_queue():
    global db_analyzed_articles, db_metric_data_daily, db_metric_data_monthly,\
            connection, consume_channel

    # Init DB
    db_analyzed_articles = mongo.get_analyzed_articles()
    db_metric_data_daily = mongo.get_metric_data_daily()
    db_metric_data_monthly = mongo.get_metric_data_monthly()

    # Init connection and channels to RabbitMQ
    connection, consume_channel = queue.init_connection()
    queue.init_analyzed_articles(consume_channel)


def analyzed_articles_consumer(channel, method, header, body):
    ___(body)
    channel.basic_ack(delivery_tag = method.delivery_tag)


def start_consuming_analyzed_articles():
    try:
        print "  Metric Writer Started..."
        queue.consume_analyzed_articles(consume_channel,
                analyzed_articles_consumer)
    finally:
        queue.close_connection(connection)


def create_metrics_for_article(article_id, preview=False):
    global metricized_articles

    # Get article from the DB...
    analyzed_article = read_analyzed_article_from_db(article_id)

    # Create metrics...
    if analyzed_article:
        # TODO...
        print "  TODO: Create metrics for article %s" % article_id
    else:
        print "  ERROR: No document with id of '%s' in DB" % article_id



def read_analyzed_article_from_db(article_id):
    analyzed_article = db_analyzed_articles.find_one({ "_id" : article_id})
    return analyzed_article


def parse_args():
    """ Parse the command line arguments

    """
    global verbose, updt_freq

    parser = argparse.ArgumentParser(description="Listens to queue for\
            analyzed articles to create metrics for, or optionally\
            create metrics for a given article id argument")
    parser.add_argument("-v", "--verbose", action='store_true',
            help="Make the operation talkative")
    parser.add_argument("-p", "--preview", action='store_true',
            help="Preview only, don't persist results.")
    parser.add_argument("-u", "--updt_freq", type=int, default=1000,
            help="Frequency to print an update")
    parser.add_argument("-i", "--id", help="Id of article to parse")
    args = parser.parse_args()   
    
    verbose = args.verbose
    updt_freq = args.updt_freq
    return args


if __name__ == "__main__":
    args  = parse_args()

    print "----------------------------------------------< metric_writer >----"
    init_db_and_queue()

    # If an article id is provided as an argument, create metrics for
    # it. Otherwise, start consuming msgs from the queue.
    if args.id:
        create_metrics_for_article(args.id.strip(), args.preview)
    else:
        start_consuming_analyzed_articles()