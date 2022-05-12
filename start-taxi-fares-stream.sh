#!/usr/bin/bash

( zcat data/nycTaxiFares.gz \
  | split -l 10000 --filter="kafka/bin/kafka-console-producer.sh \
    --broker-list localhost:9092 --topic taxifares; sleep 0.1" \
  > /dev/null ) &