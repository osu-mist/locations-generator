# Locations Generator ![python](https://img.shields.io/badge/python-3.7-blue.svg)

The Python script to generate locations data for the [Locations API](https://github.com/osu-mist/locations-frontend-api).

## Configuration

1. Start the API which should be tested locally.
2. copy
[configuration-example.yaml](./configuration-example.yaml) as `configuration.yaml`  and modify it as necessary.

## Usage

1. Fetch contrib files with `git-submodule`:

    ```shell
    $ git submodule update --init
    ```


2. Install dependencies via pip:

    ```shell
    $ pip install -r requirements.txt
    ```

3. Build the artifacts:

    ```shell
    $ python build-artifacts.py --config=configuration.yaml
    ```

    If the script runs successfully, the following two files will be generated in the `./build` directory:

    * `locations_combined.json` - Combined OSU locations list from various data sources
    * `services.json` - OSU services data list

4. Update AWS Elasticsearch instance:

    ```shell
    $ python es_manager.py --config=configuration.yaml
    ```
