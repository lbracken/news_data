# -*- coding: utf-8 -*-
"""
    news_data.pipeline.article_parser
    ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    [Pipeline Step 2 of 4]   

    This module subscribes to the 'scanned_files' queue and listens for jobs.
    When it receives a job, it reads the given file (article) and parses it
    into a structured form, removing all HTML markup.  This content is then
    written to the DB and a notification is published to the 'parsed_articles'
    queue.

    :license: MIT, see LICENSE for more details.
"""

from db import mongo
import queue


if __name__ == "__main__":
    print '---------------------------------------------< article_parser >----'