# -*- coding: utf-8 -*-
"""
    news_data.pipeline.metric_writer
    ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    [Pipeline Step 4 of 4]   

    This module subscribes to the 'analyzed_articles' queue and listens for
    jobs. When it receives a job, it reads the analyzed article results and
    creates metric data.  This is then written to the DB.

    :license: MIT, see LICENSE for more details.
"""

from db import mongo
import queue


if __name__ == "__main__":
    print '----------------------------------------------< metric_writer >----'