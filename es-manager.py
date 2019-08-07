import io
import json
import logging

from elasticsearch import Elasticsearch, RequestsHttpConnection, helpers
from pprint import pformat

from utils import load_json, load_yaml


class ESManager:
    def __init__(self):
        self.config = load_yaml('configuration.yaml')['aws_elasticsearch']
        self.es = Elasticsearch(
            hosts=[{'host': self.config['host'], 'port': self.config['port']}],
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
                _source=False
            )
            self.old_ids[index] = set([doc['_id'] for doc in scan])
        self.bulk_body = {
            'locations': io.StringIO(),
            'services': io.StringIO()
        }

    def create_or_update_doc(self, index, doc):
        body = self.bulk_body[index]
        body.write(json.dumps({'index': {'_id': doc['id']}}))
        body.write('\n')
        body.write(json.dumps(doc))
        body.write('\n')

    def delete_doc(self, index, doc_id):
        body = self.bulk_body[index]
        body.write(json.dumps({'delete': {'_id': doc_id}}))
        body.write('\n')

    def bulk_docs(self, index):
        return self.es.bulk(
            body=self.bulk_body[index].getvalue(),
            index=index,
            doc_type=index
        )


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, format='%(message)s')
    logging.getLogger('elasticsearch').setLevel(logging.WARNING)

    es_manager = ESManager()
    locations = load_json('build/locations-combined.json')
    services = load_json('build/services.json')

    for index, docs in [('locations', locations), ('services', services)]:
        new_ids = set()
        old_ids = es_manager.old_ids[index]
        for doc in docs:
            doc_id = doc['id']
            new_ids.add(doc_id)
            if doc_id not in old_ids:
                logging.info(f'[CREATE] {index} {doc_id}')
            else:
                logging.info(f'[UPDATE] {index} {doc_id}')
            es_manager.create_or_update_doc(index, doc)

        create_ids = new_ids - old_ids
        update_ids = new_ids.intersection(old_ids)
        delete_ids = old_ids - new_ids
        for delete_id in delete_ids:
            logging.info(f'[DELETE] {index} {delete_id}')
            es_manager.delete_doc(index, delete_id)

        logging.info(
            f'{"-" * 50}\n'
            f'Index: {index}\n'
            f'Number of creating document: {len(create_ids)}\n'
            f'Number of updating document: {len(update_ids)}\n'
            f'Number of deleting document: {len(delete_ids)}\n'
            f'Size of old ES instance: {len(old_ids)}\n'
            f'Size of new ES instance: {len(docs)}\n'
            f'{"-" * 50}\n'
        )
        result = es_manager.bulk_docs(index)
        logging.info(pformat(result))
