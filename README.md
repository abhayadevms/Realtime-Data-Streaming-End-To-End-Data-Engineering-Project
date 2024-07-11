# Realtime-Data-Streaming-End-To-End-Data-Engineering-Project




```bash
docker exec -it realtime-data-streaming-end-to-end-data-engineering-project-spark-master-1 \
    /opt/bitnami/spark/bin/spark-submit \
    --master spark://spark-master:7077 \
    --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,org.postgresql:postgresql:42.2.18 \
    spark-worker.py
```