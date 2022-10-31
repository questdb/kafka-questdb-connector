FROM ubuntu:latest AS builder
WORKDIR /opt
RUN apt-get update && apt-get install -y curl wget unzip jq
RUN curl -s https://api.github.com/repos/questdb/kafka-questdb-connector/releases/latest | jq -r '.assets[]|select(.content_type == "application/zip")|.browser_download_url'|wget -qi -
RUN unzip kafka-questdb-connector-*-bin.zip

FROM confluentinc/cp-kafka-connect:7.2.1
COPY --from=builder /opt/kafka-questdb-connector/*.jar /usr/share/java/kafka/