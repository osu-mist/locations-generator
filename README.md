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

3. Run the integration test:

  $ python app.py --config=configuration.yaml
  ```

## Docker

1. Build the docker image:

    ```shell
    $ docker build -t locations-generator .
    ```

2. Run the app in a container:

    ```shell
    $ docker run -v "$PWD"/configuration.yaml:/usr/src/app/configuration.yaml:ro \
                -v "$PWD"/build:/usr/src/app/build \
                locations-generator
    ```
