# -*- coding: utf-8 -*-
"""
    news_data.pipeline.article_analyzer
    ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    [Pipeline Step 3 of 4]   

    By default, this module subscribes to the 'parsed_articles' queue
    and listens for jobs. When it receives a job, it reads the parsed
    article and performs various analysis on the content.  The results
    are then written to the DB and a notification is published to the
    'analyzed_articles' queue.

    This module can also be invoked from the command line to analyze a
    single provided article (by id) instead of listening to the queue
    for jobs.  See --help for details.

    :license: MIT, see LICENSE for more details.
"""

import argparse
from datetime import datetime
import string

from db import mongo
import queue


verbose = False
updt_freq = 1000
stop_words = None

articles_analyzed = 0
connection = None
consume_channel = None
publish_channel = None
db_parsed_articles = None
db_analyzed_articles = None


def init_db_and_queue():
    global db_parsed_articles, db_analyzed_articles, connection, \
            consume_channel, publish_channel

    # Init DB
    db_parsed_articles = mongo.get_parsed_articles()
    db_analyzed_articles = mongo.get_analyzed_articles()

    # Init connection and channels to RabbitMQ
    connection, consume_channel = queue.init_connection()
    queue.init_parsed_articles(consume_channel)

    publish_channel = connection.channel()
    queue.init_analyzed_articles(publish_channel, True)


def parsed_articles_consumer(channel, method, header, body):
    analyze_article(body)
    channel.basic_ack(delivery_tag = method.delivery_tag)


def start_consuming_parsed_articles():
    try:
        print "  Article Analyzer Started..."
        queue.consume_parsed_articles(consume_channel,
                parsed_articles_consumer)
    finally:
        queue.close_connection(connection)


def analyze_article(article_id, preview=False):
    global articles_analyzed

    # Get article from the DB...
    parsed_article = read_parsed_article_from_db(article_id)

    # Perform term analysis
    if parsed_article:

        # TODO: Remove numbers?
        # TODO: Remove phrases like:   (commercial break) (unintelligble)
        # TODO: Remove speaker at begining?

        term_histogram = {}
        total_terms_count = 0

        article_id = parsed_article["_id"]
        content = parsed_article["content"]

        for line in content.splitlines():
            terms_in_line = get_term_from_line(line)
            terms_in_line = remove_stop_words(terms_in_line)

            #TODO: Word stemming would go here...
        
            for term in terms_in_line:
                term_histogram[term] = term_histogram.get(term, 0) + 1
                total_terms_count += 1

        unique_terms = term_histogram.keys()
        unique_terms.sort()

        # Created analyzed_article result
        analyzed_article = {
            "_id"          : article_id,
            "source"       : parsed_article["source"],
            "published"    : parsed_article["published"],
            "title"        : parsed_article["title"],

            "unique_terms"       : unique_terms,
            "unique_terms_count" : len(unique_terms),
            "total_terms_count"  : total_terms_count,
            "term_histogram"     : term_histogram
        }

        # Write analyzed article to DB.
        if preview:
            print "  !! Preview Only !!"
            print ""
        else:        
            add_analyzed_article_to_db(analyzed_article)
            add_analyzed_article_to_queue(publish_channel, article_id)

        # Update status...
        articles_analyzed += 1
        if articles_analyzed % updt_freq == 0:
            print "    * Articles Analyzed: %d..." % articles_analyzed

        if verbose:
            print "  Parsed File (_id): %s" % analyzed_article["_id"]
            print "  Source:            %s" % analyzed_article["source"]
            print "  Published:         %s" % analyzed_article["published"]
            print "  Title:             %s" % analyzed_article["title"]
            print "  Content length:    {:,}".format(len(content))
            print ""
            print "  Unique Terms: %d" % len(analyzed_article["unique_terms"])
            print "  Total Words:  %d" % sum(analyzed_article["term_histogram"].values())
            print ""
            print "  Top 25 terms:"
            print_common(term_histogram, 500)
            print ""


    else:
        print "  ERROR: No document with id of '%s' in DB" % article_id


def get_term_from_line(line):

    line = line.replace('-', ' ')
    line = line.replace(':', ' ')

    terms = []
    for term in line.split():
        term = term.strip(string.punctuation + string.whitespace)
        term = term.lower().strip()

        # MongoDB doesn't allow dots in keys, so encode all dots
        term = term.replace(".", "(dot)")

        if len(term) > 0:
            terms.append(term)

    return terms


def remove_stop_words(line_to_modify):
    global stop_words

    # Initialize stop_words if not already set
    if not stop_words:
        stop_words = []
        fin = open("pipeline/stop_words.txt", 'r')

        for line in fin:
            line = line.strip()

            # Ignore comment lines...
            if(line.startswith('#')):
                continue
            stop_words.append(line)

        fin.close()

    return [item for item in line_to_modify if item not in stop_words]


def print_common(word_hist, top_n):
    frequency = []
    for key, value in word_hist.items():
        frequency.append((value, key))

    frequency.sort(reverse=True)

    if top_n > len(word_hist):
        top_n = len(word_hist)

    for idx in range(top_n):
        word = frequency[idx][1]
        print "    %d) %s   Count: %s" % (idx, word, word_hist[word])


def read_parsed_article_from_db(article_id):
    parsed_article = db_parsed_articles.find_one({ "_id" : article_id})
    return parsed_article


def add_analyzed_article_to_db(analyzed_article):
    analyzed_article_id = db_analyzed_articles.save(analyzed_article, w=1)
    return analyzed_article_id


def add_analyzed_article_to_queue(channel, db_id):
    return queue.publish_analyzed_article(publish_channel, str(db_id))
    return True


def parse_args():
    """ Parse the command line arguments

    """
    global verbose, updt_freq

    parser = argparse.ArgumentParser(description="Listens to queue for\
            articles to analyze, or optionally analyze a given article\
            id arguement.")
    parser.add_argument("-v", "--verbose", action='store_true',
            help="Make the operation talkative")
    parser.add_argument("-p", "--preview", action='store_true',
            help="Preview only, don't persist results.")
    parser.add_argument("-u", "--updt_freq", type=int, default=1000,
            help="Frequency to print an update")
    parser.add_argument("-i", "--id", help="Id of article to analyze")
    args = parser.parse_args()   
    
    verbose = args.verbose
    updt_freq = args.updt_freq
    return args


if __name__ == "__main__":
    args  = parse_args()

    print "-------------------------------------------< article_analyzer >----"
    init_db_and_queue()

    # If an article id is provided as an argument, analyze that.
    # Otherwise, start consuming msgs from the queue.
    if args.id:
        analyze_article(args.id.strip(), args.preview)
    else:
        start_consuming_parsed_articles()
