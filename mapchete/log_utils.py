#!/usr/bin/env python

import os

def get_log_config(mapchete_file):

    log_dir = os.path.dirname(mapchete_file)
    log_file = os.path.join(log_dir, "mapchete.log")
    log_config = {
        'version': 1,
        'disable_existing_loggers': True,
        'formatters': {
            'simple': {
                'format': '%(levelname)s: %(message)s'
            },
            'verbose': {
                'format': '[%(asctime)s][%(module)s] %(levelname)s: %(message)s'
            }
        },
        'handlers': {
            'file': {
                'level': 'DEBUG',
                'class': 'logging.handlers.WatchedFileHandler',
                'filename': log_file,
                'formatter': 'verbose',
                'filters': [],
            },
            'stream': {
                'level': 'INFO',
                'class': 'logging.StreamHandler',
                'formatter': 'simple',
                'filters': [],
            }
        },
        'loggers': {
            'mapchete': {
                'handlers': ['file', 'stream'],
                'level': 'DEBUG',
                'propagate': True
            }
        }
    }
    return log_config
