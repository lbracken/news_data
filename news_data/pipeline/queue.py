# -*- coding: utf-8 -*-
"""
    news_data.pipeline.queue
    ~~~~~~~~~~~~~~~~~~~~~~~~ 

    This module supports interaction with RabbitMQ.

    Usage: Provides three functions for each exhange/queue pair.

        * init_<queue_name> : Declares the queue, exchange and binding
        * publish_<queue_name> : Publishes a msg to the given queue/exchange
        * consume_<queue_name> : Start consuming msgs from the given queue
    
    :license: MIT, see LICENSE for more details.
"""