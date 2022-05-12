#!/usr/bin/bash

( zcat data/nycTaxiRides.gz \
  | split -l 10000 --filter="kafka/bin/kafka-console-producer.sh \
    --broker-list localhost:9092 --topic taxirides; sleep 0.1" \
  > /dev/null ) &