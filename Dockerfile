FROM sbtscala/scala-sbt:eclipse-temurin-focal-17.0.10_7_1.9.9_3.3.1 as builder
COPY bin /tmp/bin
COPY project /tmp/project
COPY src /tmp/src
COPY build.sbt /tmp/

RUN cd /tmp && sbt assembly

FROM openjdk:17-jdk-slim as prod
ENV LANG=C.UTF-8 LANGUAGE=C.UTF-8 LC_ALL=C.UTF-8
RUN sed -i -E 's/(security|deb)\.debian\.org/mirrors.aliyun.com/g' /etc/apt/sources.list \
    && apt-get clean && apt-get update \
    && apt-get -y install tini

COPY --from=builder /tmp/bin/* /opt/package/tinylsm/
COPY --from=builder /tmp/target/scala-3.3.1/TinyLsmAssembly.jar /opt/package/tinylsm/TinyLsmAssembly.jar

RUN ln -s /opt/package/tinylsm/tinylsm-cli /usr/bin/tinylsm-cli
RUN mkdir -p /etc/tinylsm/data
WORKDIR /opt/package/tinylsm
ENTRYPOINT ["tini", "--"]
CMD ["bash", "/opt/package/tinylsm/tinylsm"]