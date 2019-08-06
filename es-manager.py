import io
import json

from elasticsearch import Elasticsearch, RequestsHttpConnection, helpers

from utils import load_json, load_yaml


class ESManager:
    def __init__(self):
        self.config = load_yaml('configuration.yaml')['aws_elasticsearch']
        self.es = Elasticsearch(
            hosts=[{'host': self.config['host'], 'port': 443}],
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
        self.bulk_body = {'locations': io.StringIO(), 'services': io.StringIO()}

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
    es_manager = ESManager()
    locations = load_json('build/locations-combined.json')

    body = io.StringIO()
    new_ids = set()
    for location in locations:
        location_id = location['id']
        new_ids.add(location_id)
        if location_id not in es_manager.old_ids['locations']:
            print(f'CREATE: location {location_id}')
        else:
            print(f'UPDATE: location {location_id}')
        es_manager.create_or_update_doc('locations', location)

    delete_ids = es_manager.old_ids['locations'] - new_ids
    print(f'locations {delete_ids} will be deleted. ({len(delete_ids)} in total)')
    for delete_id in delete_ids:
        es_manager.delete_doc('locations', delete_id)

    print(es_manager.bulk_docs('locations'))
