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
from datetime import timedelta

import date_util
from db import mongo
import queue

# >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
# Temp -- used for monitoring performance
# Daily write/read/create time
dw_time = timedelta(days=0)
dr_time = timedelta(days=0)
dc_time = timedelta(days=0)
# Monthly write/read/create time
mw_time = timedelta(days=0)
mr_time = timedelta(days=0)
mc_time = timedelta(days=0)
# <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<

verbose = False
updt_freq = 1000

metricized_articles = 0
terms_processed = 0
docs_created_daily = 0
docs_created_monthly = 0

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
    create_metrics_for_article(body)
    channel.basic_ack(delivery_tag = method.delivery_tag)


def start_consuming_analyzed_articles():
    try:
        print "  Metric Writer Started..."
        queue.consume_analyzed_articles(consume_channel,
                analyzed_articles_consumer)
    finally:
        queue.close_connection(connection)


def create_metrics_for_article(article_id, preview=False):
    global metricized_articles, terms_processed

    # Get article from the DB...
    analyzed_article = read_analyzed_article_from_db(article_id)

    # Create metrics...
    if analyzed_article:
        # There are a few different approaches to consider when writing
        # metric data.
        # (1) Ensure documents are allocated, and then upsert data for
        #     the change
        # (2) Upsert an entire doc each time, where all values are zero
        #     except one.
        # (3) Upsert daily term docs one at a time, then aggregate into
        #     higher level data later
        #
        # >> Currently selecting to do the former approach.  Results in
        #    more small reads to the DB, but smaller writes.
    
        # Get needed date values
        published = analyzed_article["published"]
        yyyy = published.year
        mm = published.month
        dd = published.day
        first_of_month = datetime(yyyy, mm, 1)
        days_in_curr_month = date_util.get_days_in_month(yyyy, published.month)

        # Iterate over each term in the term histogram
        term_histogram = analyzed_article["term_histogram"]
        for term in term_histogram:
            terms_processed += 1

            if not preview:
                update_daily_metrics(term, yyyy, mm, dd, first_of_month,
                    days_in_curr_month, term_histogram[term])
                update_monthly_metrics(term, yyyy, mm, term_histogram[term])

        # Increase count and update status after each article...
        metricized_articles += 1
        if preview or metricized_articles % updt_freq == 0:
            print "  * Articles Metricized: %d..." % metricized_articles
            print "      Terms: %d    Daily Docs %d    Monthly Docs %d" % \
                    (terms_processed, docs_created_daily, docs_created_monthly)
            print "      Monthly:  Read: %s,  Create: %s,  Write: %s" % \
                    (mr_time, mc_time, mw_time)
            print "      Daily:    Read: %s,  Create: %s,  Write: %s" % \
                    (dr_time, dc_time, dw_time)

    else:
        print "  ERROR: No document with id of '%s' in DB" % article_id




def read_analyzed_article_from_db(article_id):
    analyzed_article = db_analyzed_articles.find_one({ "_id" : article_id})
    return analyzed_article


def update_daily_metrics(term, yyyy, mm, dd, first_of_month,
        days_in_curr_month, term_count):
    global docs_created_daily, dr_time, dw_time, dc_time
    

    # Create the metric identifier
    id_daily_metric = {
        "_id" : {
            "term" : term,
            "yyyy" : yyyy,
            "mm"   : date_util.pad_month_day_value(mm)
        }
    }

    # Check if a doc for this identifier already exists, if not
    # allocate the doc
    r_time = datetime.now()
    if (db_metric_data_daily.find(id_daily_metric).count() == 0):
        dr_time += (datetime.now() - r_time)
        c_time = datetime.now()

        docs_created_daily += 1
        metric_doc_daily = {
            "_id"  : id_daily_metric["_id"],
            "term" : term,
            "date" : first_of_month,
            "daily": {}
        }

        for day in range(1, days_in_curr_month + 1):
            metric_doc_daily["daily"][str(day)] = 0

        db_metric_data_daily.insert(metric_doc_daily)       
        dc_time += (datetime.now() - c_time)
    else:
        dr_time += (datetime.now() - r_time)

    # Update the daily metric data with this value
    w_time = datetime.now()
    metric_update_daily = {"$inc" : {"daily." + str(dd) : term_count}}
    db_metric_data_daily.update(id_daily_metric, metric_update_daily,
            True)  # True for upsert
    dw_time +=  (datetime.now() - w_time)


def update_monthly_metrics(term, yyyy, mm, term_count):
    global docs_created_monthly, mr_time, mw_time, mc_time
    
    # Create the metric identifier
    id_monthly_metric = {
        "_id" : {
            "term" : term
        }
    }

    # Check if a doc for this identifier already exists, if not
    # allocate the doc
    r_time = datetime.now()
    if (db_metric_data_monthly.find(id_monthly_metric).count() == 0):
        mr_time += (datetime.now() - r_time)
        c_time = datetime.now()

        docs_created_monthly += 1
        metric_doc_monthly = {
            "_id"  : id_monthly_metric["_id"],
            "term" : term
        }

        for yyyy in range(2000, 2014):
            metric_doc_monthly[str(yyyy)] = {}
            for mm in range(1, 13):
                metric_doc_monthly[str(yyyy)][str(mm)] = 0

        db_metric_data_monthly.insert(metric_doc_monthly)        
        mc_time += (datetime.now() - c_time)

    else:
        mr_time += (datetime.now() - r_time)

    # Update the monthly metric data with this value
    w_time = datetime.now()
    metric_update_monthly = {"$inc" : {str(yyyy) + "." + str(mm) : term_count}}
    db_metric_data_monthly.update(id_monthly_metric,
            metric_update_monthly, True)  # True for upsert
    mw_time +=  (datetime.now() - w_time)


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