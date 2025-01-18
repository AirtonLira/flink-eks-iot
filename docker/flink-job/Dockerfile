FROM flink:latest

RUN apt-get update && \
    apt-get install -y python3 python3-pip && \
    pip3 install apache-flink pandas pyarrow

COPY flink_job.py /opt/flink/flink_job.py

CMD ["python3", "/opt/flink/flink_job.py"]
