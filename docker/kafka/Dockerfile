FROM ubuntu:latest

ENV DEBIAN_FRONTEND noninteractive

ADD usr /usr

RUN apt update && apt install -y software-properties-common && add-apt-repository -y ppa:webupd8team/java && apt-get update

# Auto-accept the Oracle JDK license
RUN echo oracle-java8-installer shared/accepted-oracle-license-v1-1 select true | /usr/bin/debconf-set-selections

RUN apt-get install -y oracle-java8-installer curl mc htop

ENV JAVA_HOME /usr/lib/jvm/java-8-oracle

RUN wget -qO - http://packages.confluent.io/deb/3.0/archive.key | apt-key add -

RUN add-apt-repository "deb [arch=amd64] http://packages.confluent.io/deb/3.0 stable main"

RUN apt-get update && apt-get install -y confluent-kafka-2.11

EXPOSE 2181 9092
CMD zookeeper-server-start /etc/kafka/zookeeper.properties && sleep 5 && kafka-server-start /etc/kafka/server.properties

