FROM alpine:3.6
MAINTAINER Stratio "governance@stratio.com"

COPY . /

ENV MODULE='dg-indexer'
ENV JAR=${MODULE}'*-allinone.jar'
ENV USER='governance'
ENV GROUP='stratio'
ENV JAVA_HOME /usr/lib/jvm/java-1.8-openjdk

ADD http://sodio.stratio.com/repository/paas/kms_utils/0.4.0/kms_utils-0.4.0.sh /docker/kms_utils.sh
ADD http://sodio.stratio.com/repository/paas/log_utils/0.4.0/b-log-0.4.0.sh /docker/b-log.sh

WORKDIR /

ARG CACHEBUST
RUN apk update && \
    apk add binutils=2.30-r1 openjdk8-jre-base=8.212.04-r0 su-exec=0.2-r0 curl=7.61.1-r2 bash=4.3.48-r1 jq=1.5-r4 openssl=1.0.2r-r0 && \
    apk add --no-cache nss=3.30.1-r0 && \
    addgroup ${GROUP} && \
    adduser -H -h /home/${USER} -G ${GROUP} -s /bin/nologin ${USER} -D && \
    mkdir -p /opt/sds/${MODULE} && \
    mkdir -p /etc/sds/${MODULE}   && \
    chown root:${GROUP} /opt/sds /etc/sds && \
    chmod -R 0775 /opt/sds /etc/sds && \
    cp /dgindexer/target/${JAR} /opt/sds/${MODULE}/${JAR} && \
    cp /dgindexer/target/classes/logback.xml /etc/sds/${MODULE}/logback.xml && \
    chown -R ${USER}:${GROUP} /opt/sds/${MODULE} && \
    chmod -R 0755 /opt/sds/${MODULE} && \
    chown -R ${USER}:${GROUP} /etc/sds/${MODULE} && \
    chmod -R 0750 /etc/sds/${MODULE} && \
    chmod -R 0640 /etc/sds/${MODULE}/logback.xml /opt/sds/${MODULE}/${JAR} && \
    chmod +x /docker/docker-entrypoint.sh

ENTRYPOINT ["/docker/docker-entrypoint.sh"]