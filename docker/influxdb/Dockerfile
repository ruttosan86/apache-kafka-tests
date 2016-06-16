FROM ubuntu:xenial

ENV DEBIAN_FRONTEND noninteractive

RUN apt update && apt install -y curl apt-transport-https

RUN curl -sL https://repos.influxdata.com/influxdb.key | apt-key add -


RUN echo "deb https://repos.influxdata.com/ubuntu xenial stable" | tee /etc/apt/sources.list.d/influxdb.list

RUN apt update && apt install -y influxdb

COPY influxdb.conf /etc/influxdb/influxdb.conf

EXPOSE 8083 8086

VOLUME /var/lib/influxdb

COPY entrypoint.sh /entrypoint.sh
CMD ["influxd"]docker 
