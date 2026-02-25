FROM bde2020/spark-master:3.0.1-hadoop3.2

ENV LIVY_VERSION=0.8.0-incubating
ENV LIVY_HOME=/opt/livy

RUN apk add --no-cache wget unzip python3 py3-pip && \
    wget -q "https://archive.apache.org/dist/incubator/livy/${LIVY_VERSION}/apache-livy-${LIVY_VERSION}_2.12-bin.zip" \
         -O /tmp/livy.zip && \
    unzip -q /tmp/livy.zip -d /opt && \
    mv /opt/apache-livy-${LIVY_VERSION}_2.12-bin ${LIVY_HOME} && \
    rm /tmp/livy.zip && \
    mkdir -p ${LIVY_HOME}/logs

COPY livy.conf ${LIVY_HOME}/conf/livy.conf
COPY livy-env.sh ${LIVY_HOME}/conf/livy-env.sh

EXPOSE 8998

# Called with no argument, livy-server runs `exec $command` (foreground)
CMD ["/opt/livy/bin/livy-server"]
