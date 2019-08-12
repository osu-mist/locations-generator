import io
import json
import logging

from elasticsearch import Elasticsearch, RequestsHttpConnection, helpers
from pprint import pformat
from requests_aws4auth import AWS4Auth
from tabulate import tabulate

from utils import load_json, load_yaml, parse_arguments


class ESManager:
    def __init__(self, config):
        config = load_yaml(config)['aws_elasticsearch']
        self.es = Elasticsearch(
            hosts=[{'host': config['host'], 'port': config['port']}],
            http_auth=AWS4Auth(
                config['access_id'],
                config['access_key'],
                config['region'],
                'es'
            ),
            use_ssl=True,
            verify_certs=True,
            connection_class=RequestsHttpConnection
        )
        self.old_ids = {}
        for index in ['locations', 'services']:
            scan = helpers.scan(
                self.es,
                index=index,
                doc_type=index,
                _source=False  # don't include bodies
            )
            self.old_ids[index] = set([doc['_id'] for doc in scan])
        self.bulk_body = {
            'locations': io.StringIO(),
            'services': io.StringIO()
        }

    def create_or_update_doc(self, index, doc):
        """A function to write ES query to either create or update a document

        :param index: The index of document to be created/updated
        :param doc: Document object to be created/updated
        """
        body = self.bulk_body[index]
        body.write(json.dumps({'index': {'_id': doc['id']}}))
        body.write('\n')
        body.write(json.dumps(doc))
        body.write('\n')

    def delete_doc(self, index, doc_id):
        """A function to write ES query to delete a document

        :param index: The index of document to be deleted
        :param doc_id: Document ID to be deleted
        """
        body = self.bulk_body[index]
        body.write(json.dumps({'delete': {'_id': doc_id}}))
        body.write('\n')

    def bulk_query(self, index):
        """A function to bulk the ES query

        :param index: The index key of bulk query
        """
        result = self.es.bulk(
            body=self.bulk_body[index].getvalue(),
            index=index,
            doc_type=index
        )
        logging.debug(pformat(result))


if __name__ == '__main__':
    arguments = parse_arguments()

    # Setup logging level
    if arguments.debug:
        logging.basicConfig(level=logging.DEBUG, format='%(message)s')
    else:
        logging.basicConfig(level=logging.INFO, format='%(message)s')

    logging.getLogger('elasticsearch').setLevel(logging.WARNING)

    # create ES manager instance
    es_manager = ESManager(arguments.config)

    # Load data from build artifacts
    locations = load_json('build/locations-combined.json')
    services = load_json('build/services.json')

    for index, docs in [('locations', locations), ('services', services)]:
        new_ids = set()
        old_ids = es_manager.old_ids[index]
        for doc in docs:
            doc_id = doc['id']
            new_ids.add(doc_id)
            if doc_id not in old_ids:
                # Perform a CREATE if document ID not in old ID set
                logging.info(f'[CREATE] {index} {doc_id}')
            else:
                # Perform a UPDATE if document ID in old ID set
                logging.info(f'[UPDATE] {index} {doc_id}')
            es_manager.create_or_update_doc(index, doc)

        # Calculate ID status
        create_ids = new_ids - old_ids
        update_ids = new_ids.intersection(old_ids)
        delete_ids = old_ids - new_ids

        for delete_id in delete_ids:
            # Perform a DELETE for each ID in delete ID set
            logging.info(f'[DELETE] {index} {delete_id}')
            es_manager.delete_doc(index, delete_id)

        summary_table = [
            ['index', index],
            ['number of creating document', len(create_ids)],
            ['number of updating document', len(update_ids)],
            ['number of deleting document', len(delete_ids)],
            ['size of old ES instance', len(old_ids)],
            ['size of new ES instance', len(new_ids)]
        ]
        logging.info(f"{tabulate(summary_table, tablefmt='fancy_grid')}\n")
        es_manager.bulk_query(index)
