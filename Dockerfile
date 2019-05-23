FROM ubuntu:16.04
MAINTAINER Stratio "governance@stratio.com"

ENV MODULE='dg-indexer'
ENV JAR=${MODULE}'*-allinone.jar'
ENV USER='governance'
ENV GROUP='stratio'
ENV JAVA_HOME /usr/lib/jvm/java-8-openjdk-amd64/jre

COPY ./docker /docker
COPY ./dgindexer/target/${JAR} /opt/sds/${MODULE}/${JAR}
COPY ./dgindexer/target/classes/logback.xml /etc/sds/${MODULE}/logback.xml

ADD http://sodio.stratio.com/repository/paas/kms_utils/0.4.0/kms_utils-0.4.0.sh /docker/kms_utils.sh
ADD http://sodio.stratio.com/repository/paas/log_utils/0.4.0/b-log-0.4.0.sh /docker/b-log.sh

WORKDIR /

ARG CACHEBUST
RUN apt-get update && \
    apt-get install openjdk-8-jre curl jq openssl -y && \
    addgroup ${GROUP} && \
    adduser -H -h /home/${USER} -G ${GROUP} -s /bin/nologin ${USER} -D && \
    mkdir -p /opt/sds/${MODULE} && \
    mkdir -p /etc/sds/${MODULE}   && \
    chown root:${GROUP} /opt/sds /etc/sds && \
    chmod -R 0775 /opt/sds /etc/sds && \
    chmod -R 0755 /opt/sds/${MODULE} && \
    chmod -R 0750 /etc/sds/${MODULE} && \
    chmod -R 0640 /etc/sds/${MODULE}/logback.xml /opt/sds/${MODULE}/${JAR} && \
    chmod +x /docker/docker-entrypoint.sh

ENTRYPOINT ["/docker/docker-entrypoint.sh"]