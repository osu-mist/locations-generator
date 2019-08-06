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


if __name__ == '__main__':
    es_manager = ESManager()
    locations = load_json('build/locations-combined.json')

    body = io.StringIO()
    new_ids = set()
    for location in locations:
        location_id = location['id']
        new_ids.add(location_id)
        if location_id not in es_manager.old_ids['locations']:
            print(f'location {location_id} does not exist, performing CREATE')
            body.write(json.dumps({'index': {'_id': location_id}}))
            body.write('\n')
            body.write(json.dumps(location))
            body.write('\n')
        else:
            print(f'location {location_id} has existed, performing UPDATE')
            body.write(json.dumps({'index': {'_id': location_id}}))
            body.write('\n')
            body.write(json.dumps(location))
            body.write('\n')

    delete_ids = es_manager.old_ids['locations'] - new_ids
    print(f'{len(delete_ids)} of locations will be deleted, they are {delete_ids}')
    for delete_id in delete_ids:
        body.write(json.dumps({'delete': {'_id': location_id}}))
        body.write('\n')

    response = es_manager.es.bulk(
        body=body.getvalue(),
        index='locations',
        doc_type='locations'
    )
    # print(json.dumps(response, indent=4))
