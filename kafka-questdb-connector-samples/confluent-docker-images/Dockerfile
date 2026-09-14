FROM confluentinc/cp-kafka-connect-base:7.6.0
# 0.12 is the newest version published on Confluent Hub. Newer releases, including
# the ones with the QWP (ws::) transport, are on GitHub:
# https://github.com/questdb/kafka-questdb-connector/releases
RUN confluent-hub install --no-prompt questdb/kafka-questdb-connector:0.12
