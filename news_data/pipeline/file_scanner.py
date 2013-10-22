# -*- coding: utf-8 -*-
"""
    news_data.pipeline.file_scanner
    ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    [Pipeline Step 1 of 4]   

    This module recursively scans a file system directory for news
    articles, publishing each filename it finds to the 'scanned_files'
    queue.

    :license: MIT, see LICENSE for more details.
"""

import ConfigParser
import os
import re
import sys

import queue


# Constants
scan_match_pattern = ".*html$"


def add_scanned_file_to_queue(channel, filename, verbose=False):
    """ Publish the given filename to the queue

    """
    if verbose:
        print "    Adding to Queue: %s" % filename

    return queue.publish_scanned_file(channel, filename)
    return True


def scan_directory(directory):
    """ Recursively scan the given directory file new articles

    """    
    print "  Starting scan..."
    print "  Directory: %s" % directory

    config = ConfigParser.SafeConfigParser()
    config.read("settings.cfg")

    verbose = config.getboolean("global", "verbose")
    status_update_freq = config.getint("global", "status_update_freq")


    files_queued = 0

    # Initialize a connection and channel to RabbitMQ
    connection, channel = queue.init_connection()
    queue.init_scanned_files(channel, True)
    for root, dirs, files in os.walk(directory):

        # Walk dirs and files in alphabetical order
        dirs.sort()
        files.sort()

        for file in files:
            filename = os.path.join(root, file)

            # Only queue files that match the pattern
            if re.match(scan_match_pattern, filename):
                add_scanned_file_to_queue(channel, filename, verbose)
                files_queued += 1

                # Update status...
                if files_queued % status_update_freq == 0:
                    print "    * Files Queued: %d..." % files_queued            

        # Ignore any svn dirs
        if '.svn' in dirs:
            dirs.remove('.svn')

    queue.close_connection(connection)

    print ""
    print "  ... scan complete"
    print "    Files Queued: %d" % files_queued
    print ""



if __name__ == "__main__":
    print "-----------------------------------------------< file_scanner >----"

    if len(sys.argv) > 1:
        dir_to_scan = sys.argv[1].strip()
        scan_directory(dir_to_scan)
        
    else:
        print "Usage: file_scanner DIR_TO_SCAN"