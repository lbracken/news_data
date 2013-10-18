# -*- coding: utf-8 -*-
"""
    news_data.db.mongo
    ~~~~~~~~~~~~~~~~~~ 

    This module supports interaction with mongoDB.

    Usage: Call a get_<collection_name>() function to return a connection to a
    particular mongoDB collection.  The connection should he reused to limit
    the number of concurrent connections open to mongoDB.
    
    :license: MIT, see LICENSE for more details.
"""