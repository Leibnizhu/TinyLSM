FROM openjdk:17-jdk-slim
ENV LANG=C.UTF-8 LANGUAGE=C.UTF-8 LC_ALL=C.UTF-8
RUN sed -i -E 's/(security|deb)\.debian\.org/mirrors.aliyun.com/g' /etc/apt/sources.list \
    && apt-get clean && apt-get update \
    && apt-get -y install tini

COPY bin/* /etc/tinylsm/
COPY target/scala-3.3.1/TinyLsmAssembly.jar /etc/tinylsm/TinyLsmAssembly.jar

RUN ln -s /etc/tinylsm/tinylsm-cli /usr/bin/tinylsm-cli

WORKDIR /etc/tinylsm
ENTRYPOINT ["tini", "--"]
CMD ["bash", "/etc/tinylsm/tinylsm"]