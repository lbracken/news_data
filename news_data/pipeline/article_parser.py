# -*- coding: utf-8 -*-
"""
    news_data.pipeline.article_parser
    ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    [Pipeline Step 2 of 4]   

    This module subscribes to the 'scanned_files' queue and listens for
    jobs.  When it receives a job, it reads the given file (article)
    and parses it into a structured form, removing all HTML markup.
    This content is then written to the DB and a notification is
    published to the 'parsed_articles' queue.

    :license: MIT, see LICENSE for more details.
"""

import sys

from db import mongo
import queue


publish_channel = None
db_parsed_articles = None


def init_db():
    global db_parsed_articles
    db_parsed_articles = mongo.get_parsed_articles()


def scanned_files_consumer(channel, method, header, body):
    parse_article(body)
    channel.basic_ack(delivery_tag = method.delivery_tag)


def start_consuming_scanned_files():
    """ Initialize a connection and channel to RabbitMQ, then start
        consuming messages.  The message body is an article's filename.

    """
    global publish_channel

    connection, consume_channel = queue.init_connection()
    queue.init_scanned_files(consume_channel)

    publish_channel = connection.channel()
    queue.init_parsed_articles(publish_channel, True)

    try:
        queue.consume_scanned_files(consume_channel, scanned_files_consumer)
    finally:
        queue.close_connection(connection)


def parse_article(filename):
    # TODO: Impl parse logic...
    #  1) Read the file and parse it with BeautifulSoup
    #  2) Write data to the DB
    #  3) Add a message to the queue via the publish_channel
    print " ... parsing article [%s]" % filename



if __name__ == "__main__":
    print "---------------------------------------------< article_parser >----"
    print "  Article Parser Started..."

    init_db()

    # If a filename is provided as an argument, parse that.
    # Otherwise, start consuming files from the queue.
    if len(sys.argv) > 1:
        parse_article(sys.argv[1])
        
    else:
        start_consuming_scanned_files()