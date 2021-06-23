#!/bin/bash

while [[ $# -gt 0 ]]
do
key="$1"

case $key in
    -t|--topic)
    topic="$2"
    shift # past argument
    shift # past value
    ;;
    *)    # unknown option
esac
done

echo "Creating topics"

echo "Creating ${topic}"
docker-compose exec broker kafka-topics \
  --create \
  --bootstrap-server localhost:9092 \
  --replication-factor 1 \
  --partitions 1 \
  --topic "comment-"${topic}


echo "Creating video-${topic}"

docker-compose exec broker kafka-topics \
  --create \
  --bootstrap-server localhost:9092 \
  --replication-factor 1 \
  --partitions 1 \
  --topic "video-"${topic}

echo "Creating channel-${topic}"
docker-compose exec broker kafka-topics \
  --create \
  --bootstrap-server localhost:9092 \
  --replication-factor 1 \
  --partitions 1 \
  --topic "channel-"${topic}


echo "Creating like-${topic}"
docker-compose exec broker kafka-topics \
  --create \
  --bootstrap-server localhost:9092 \
  --replication-factor 1 \
  --partitions 1 \
  --topic "like-"${topic}

echo "Creating subscription-${topic}"
docker-compose exec broker kafka-topics \
  --create \
  --bootstrap-server localhost:9092 \
  --replication-factor 1 \
  --partitions 1 \
  --topic "subscription-"${topic}


