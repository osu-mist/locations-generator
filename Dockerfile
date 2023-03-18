# LICENSE UPL 1.0
#
# Copyright (c) 2014, 2019, Oracle and/or its affiliates. All rights reserved.
#
# ORACLE DOCKERFILES PROJECT

FROM oraclelinux:7-slim
ARG oracle_release
ARG oracle_update

# Install Oracle Instant Client
RUN yum -y install oracle-release-el7 oracle-epel-release-el7 && \
    yum-config-manager --enable ol7_oracle_instantclient && \
    yum -y install oracle-instantclient18.5-basic oracle-instantclient18.5-devel oracle-instantclient18.5-sqlplus && \
    echo /usr/lib/oracle/18.5/client64/lib > /etc/ld.so.conf.d/oracle-instantclient18.5.conf && \
    ldconfig
RUN yum -y install make wget gzip gcc openssl-devel bzip2-devel libffi-devel zlib-devel && \
    wget https://www.python.org/ftp/python/3.9.15/Python-3.9.15.tgz && \
    tar -xvf Python-3.9.15.tgz && \
    cd Python-3.9.15 && \
    ./configure --enable-optimizations && \
    make altinstall && \
    python3.9 --version
ENV PATH=$PATH:/usr/lib/oracle/18.5/client64/bin
ENV PYTHONIOENCODING UTF-8

WORKDIR /usr/src/app

COPY . .

RUN python3.9 -m pip install --no-cache-dir -r requirements.txt

CMD ["sh", "./locations-generator.sh", "configuration.yaml"]
