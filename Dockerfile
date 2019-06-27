FROM python:3.7

WORKDIR /usr/src/app

COPY . .

RUN apt-get update && apt-get install -y libaio1 unzip
RUN mkdir -p /opt/oracle
RUN unzip bin/instantclient-basiclite-linux.x64-12.2.0.1.0.zip -d /opt/oracle
RUN cd /opt/oracle/instantclient_12_2 \
    && ln -s libclntsh.so.12.1 libclntsh.so \
    && ln -s libocci.so.12.1 libocci.so
RUN echo /opt/oracle/instantclient_12_2 > /etc/ld.so.conf.d/oracle-instantclient.conf \
    && ldconfig

RUN pip install --no-cache-dir -r requirements.txt

USER nobody:nogroup

CMD ["python", "app.py", "--config=configuration.yaml"]
