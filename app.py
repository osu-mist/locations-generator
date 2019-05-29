import logging

import requests

import utils


if __name__ == '__main__':
    arguments = utils.parse_arguments()

    # Setup logging level
    if arguments.debug:
        logging.basicConfig(level=logging.DEBUG)
    else:
        logging.basicConfig(level=logging.INFO)

    config = utils.load_config()
