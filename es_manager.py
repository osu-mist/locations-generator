import io
import json
import logging
import sys
from pprint import pformat

from elasticsearch import Elasticsearch, RequestsHttpConnection, helpers
from requests_aws4auth import AWS4Auth
from tabulate import tabulate

from utils import load_json, load_yaml, parse_arguments


class ESManager:
    def __init__(self, config):
        config = load_yaml(config)['awsElasticsearch']
        self.es = Elasticsearch(
            hosts=[{'host': config['host'], 'port': config['port']}],
            http_auth=AWS4Auth(
                config['accessId'],
                config['accessKey'],
                config['region'],
                'es'
            ),
            use_ssl=True,
            verify_certs=True,
            connection_class=RequestsHttpConnection
        )
        self.current_ids = {}
        for index in ['locations', 'services']:
            scan = helpers.scan(
                self.es,
                index=index,
                doc_type=index,
                _source=False  # don't include bodies
            )
            self.current_ids[index] = set([doc['_id'] for doc in scan])
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
        self.parse_bulk_errors(result)

    def parse_bulk_errors(self, result):
        if result['errors']:
            for doc in result['items']:
                doc_index = doc['index']
                if 'error' in doc_index:
                    index = doc_index['_index']
                    doc_id = doc_index['_id']
                    reason = doc_index['error']['caused_by']['reason']
                    logger.error(f"[ERROR] {index} {doc_id} '{reason}'")
            sys.exit(1)


if __name__ == '__main__':
    arguments = parse_arguments()

    # Setup logging level
    logging.basicConfig(
        level=(logging.DEBUG if arguments.debug else logging.INFO)
    )
    logger = logging.getLogger(__name__)
    # Set logging level to WARNING for the logger of elasticsearch package
    logging.getLogger('elasticsearch').setLevel(logging.WARNING)

    # create ES manager instance
    es_manager = ESManager(arguments.config)

    # Load data from build artifacts
    output_folder = 'build'
    locations = load_json(f'{output_folder}/locations-combined.json')
    services = load_json(f'{output_folder}/services.json')

    for index, docs in [('locations', locations), ('services', services)]:
        current_ids = es_manager.current_ids[index]
        artifact_ids = set()
        create_ids = set()
        update_ids = set()

        for doc in docs:
            doc_id = doc['id']
            artifact_ids.add(doc_id)
            if doc_id not in current_ids:
                # Perform a CREATE if document ID not in current ID set
                logger.info(f'[CREATE] {index} {doc_id}')
                create_ids.add(doc_id)
            else:
                # Perform a UPDATE if document ID in current ID set
                logger.info(f'[UPDATE] {index} {doc_id}')
                update_ids.add(doc_id)
            es_manager.create_or_update_doc(index, doc)

        delete_ids = current_ids - artifact_ids
        for delete_id in delete_ids:
            # Perform a DELETE for each ID in delete ID set
            logger.info(f'[DELETE] {index} {delete_id}')
            es_manager.delete_doc(index, delete_id)

        summary_table = [
            ['index', index],
            ['number of creating document', len(create_ids)],
            ['number of updating document', len(update_ids)],
            ['number of deleting document', len(delete_ids)],
            ['size of current ES instance', len(current_ids)],
            ['size of new ES instance', len(artifact_ids)]
        ]
        logger.info(f"\n{tabulate(summary_table, tablefmt='fancy_grid')}\n")
        es_manager.bulk_query(index)
