#FROM ubuntu:20.04
FROM python:3.10-slim

RUN apt-get update \
  && apt-get install -y libpq-dev unixodbc-dev

COPY ./requirements.txt /data/requirements.txt
COPY ./main.py /data/main.py
COPY ./artificats /data/artificats

WORKDIR /data

RUN python -m pip install --upgrade pip
RUN python -m pip install -r requirements.txt
RUN python -m pip install --no-cache-dir numpy

CMD ["python", "-u", "main.py"]