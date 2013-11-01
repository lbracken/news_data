# -*- coding: utf-8 -*-
"""
    news_data.pipeline.article_parser
    ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    [Pipeline Step 2 of 4]   

    By default, this module subscribes to the 'scanned_files' queue and
    listens for jobs.  When it receives a job, it reads the given file
    (article) and parses it into a structured form, removing all HTML
    markup. This content is then written to the DB and a notification
    is published to the 'parsed_articles' queue.

    This module can also be invoked from the command line to parse a
    single provided file instead of listening to the queue for jobs.
    See --help for details.

    TODO: Parser as is only works for early 2000's files from CNN, need
    to rework to support additional article formats...

    :license: MIT, see LICENSE for more details.
"""

import argparse
from datetime import datetime
from datetime import timedelta
import os
import json
import sys
import traceback

from bs4 import BeautifulSoup

from db import mongo
import queue


verbose = False
updt_freq = 1000
lines_to_ignore = None

articles_parsed = 0
connection = None
consume_channel = None
publish_channel = None
db_parsed_articles = None


def init_db_and_queue():
    global db_parsed_articles, connection, consume_channel, publish_channel

    # Init DB
    db_parsed_articles = mongo.get_parsed_articles()

    # Init connection and channels to RabbitMQ
    connection, consume_channel = queue.init_connection()
    queue.init_scanned_files(consume_channel)

    publish_channel = connection.channel()
    queue.init_parsed_articles(publish_channel, True)


def scanned_files_consumer(channel, method, header, body):
    parse_article(body)
    channel.basic_ack(delivery_tag = method.delivery_tag)


def start_consuming_scanned_files():
    """ Start consuming messages.  The message body is an article's
        filename.

    """
    try:
        print "  Article Parser Started..."
        queue.consume_scanned_files(consume_channel, scanned_files_consumer)
    finally:
        queue.close_connection(connection)


def parse_article(filename, preview=False):
    """ Read the given file and parse its content.

    """
    global articles_parsed
    
    try:
        # Read the file and parse with BeautifulSoup
        # The 2nd arg specifies that the lxml parser should be used
        filename = filename.strip()
        soup = BeautifulSoup(open(filename), "lxml")

        # Parse various facets of the article
        article = {
            "_id"       : filename,
            "source"    : "CNN",
            "published" : parse_date(filename),
            "title"     : parse_title(soup),
            "content"   : parse_content(filename, soup)
        }

        # Calculate parsed content size
        size_raw = os.stat(filename).st_size
        size_parsed = len(article["content"])
        article["size_raw"] = size_raw
        article["size_parsed"] = size_parsed
        article["size_ratio"] = 0 if size_parsed <= 0 else\
                float(size_raw) / float(size_parsed)

        # Write parsed file to DB.
        if preview:
            print "  !! Preview Only !!"
            print ""
        else:
            add_article_to_db(article)
            add_article_to_queue(publish_channel, article["_id"])

        # Update status...
        articles_parsed += 1
        if articles_parsed % updt_freq == 0:
            print "    * Articles Parsed: %d..." % articles_parsed    

        if verbose:
            print "  Parsed File (_id): %s" % article["_id"]
            print "  Source:            %s" % article["source"]
            print "  Published:         %s" % article["published"]
            print "  Title:             %s" % article["title"]
            print "  Size (Raw):        {:,}".format(article["size_raw"])
            print "  Size (Parsed):     {:,}".format(article["size_parsed"])
            print "  Size (Ratio):      {:,}".format(article["size_ratio"])
            print ""
            print article["content"]

    except Exception, e: 
        print "!! Failed parsing the file: %s" % filename
        print "  %s" % str(e)
        if verbose:
            traceback.print_exc()


def parse_date(filename):
    """ Get the published date from the filename

    """
    parsed_filename = filename.split('/')
    prgm_year  = int(parsed_filename[-3][:2]) + 2000
    prgm_month = int(parsed_filename[-3][-2:])
    prgm_day   = int(parsed_filename[-2])

    return datetime(prgm_year, prgm_month, prgm_day)


def parse_title(soup):
    """ Get the article title from the soup

    """
    # Use the title of the page.
    title = soup.title.string.encode('utf-8').strip().lower()

    # Remove trailing date for titles of format:
    #   "CNN Transcript - Title - Jan 1, 2000"
    if title.startswith('cnn transcript'):
        parsed_title = title.split('-')
        if len(parsed_title) > 2:
            title = ''.join(parsed_title[:-1])

    # Remove any text before the first ':', e.g. 'Breaking News:' 
    parsed_title = title.split(':')
    if len(parsed_title) > 1:
            title = ':'.join(parsed_title[1:])

    # The following strings should be removed from the title
    to_remove = [
        'cnn transcript',
        'cnn'   # <--- This item should be last
    ]

    for string in to_remove:
        title = title.replace(string, '')

    # Strip out certain special chars and strip extra whitespace
    title = title.strip().strip('-:')
    return title.strip()


def parse_content(filename, soup):
    """ Get the content of the article

    """
    content_lines = []
    for line in soup.find_all('p'):

        if line == None or line.string == None:
            continue;

        line = line.string.encode('utf-8').strip().lower()

        # See if line should be ignored
        if ignore_content_line(line):
            continue

        content_lines.append(line)

    return '\n'.join(content_lines)


def ignore_content_line(line):
    global lines_to_ignore

    # Initialize lines_to_ignore if not already set
    if not lines_to_ignore:
        lines_to_ignore = []
        fin = open("pipeline/lines_to_ignore.txt", 'r')

        for line in fin:
            line = line.strip()

            # Ignore comment lines...
            if(line.startswith('#')):
                continue
            lines_to_ignore.append(line)

        fin.close()

    for ignore in lines_to_ignore:
        if line.startswith(ignore):
            return True
    return False


def add_article_to_db(article):
    article_id = db_parsed_articles.save(article, w=1)
    return article_id


def add_article_to_queue(channel, db_id):
    return queue.publish_parsed_article(publish_channel, str(db_id))
    return True


def parse_args():
    """ Parse the command line arguments

    """
    global verbose, updt_freq

    parser = argparse.ArgumentParser(description="Listens to queue for files\
            to parse, or optionally parse a given filename arguement.")
    parser.add_argument("-v", "--verbose", action='store_true',
            help="Make the operation talkative")
    parser.add_argument("-p", "--preview", action='store_true',
            help="Preview only, don't persist results.")
    parser.add_argument("-u", "--updt_freq", type=int, default=1000,
            help="Frequency to print an update")      
    parser.add_argument("-f", "--filename", help="Filename to parse")
    args = parser.parse_args()   
    
    verbose = args.verbose
    updt_freq = args.updt_freq
    return args


if __name__ == "__main__":
    args  = parse_args()

    print "---------------------------------------------< article_parser >----"

    if not args.preview:
        init_db_and_queue()

    # If a filename is provided as an argument, parse that.
    # Otherwise, start consuming files from the queue.
    if args.filename:
        parse_article(args.filename.strip(), args.preview)
    else:
        start_consuming_scanned_files()