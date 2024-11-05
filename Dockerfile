FROM ubuntu:latest

RUN apt-get update \
  && apt-get install -y python3-pip python3-dev libpq-dev unixodbc-dev \
  && cd /usr/local/bin \
  && ln -s /usr/bin/python3 python \
  && pip3 install --upgrade pip

COPY ./requirements.txt /data/requirements.txt
COPY ./main.py /data/main.py
COPY ./config.json /data/config.json

WORKDIR /data

RUN python3 -m pip install --upgrade pip
RUN python3 -m pip install -r requirements.txt

CMD ["python", "-u", "main.py"]