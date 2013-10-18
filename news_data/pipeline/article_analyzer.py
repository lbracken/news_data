# -*- coding: utf-8 -*-
"""
    news_data.pipeline.article_analyzer
    ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    [Pipeline Step 3 of 4]   

    This module subscribes to the 'parsed_articles' queue and listens for jobs.
    When it receives a job, it reads the parsed article and performs various
    analysis on the content.  The results are then written to the DB and a
    notification is published to the 'analyzed_articles' queue.  

    :license: MIT, see LICENSE for more details.
"""

from db import mongo
import queue


if __name__ == "__main__":
    print '-------------------------------------------< article_analyzer >----'