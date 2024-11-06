FROM ubuntu:22.04

RUN apt-get update \
  && apt-get install -y libpq-dev unixodbc-dev python3-pip python3-dev \
  && pip3 install --upgrade pip

COPY ./requirements.txt /data/requirements.txt
COPY ./main.py /data/main.py
COPY ./artificats /data/artificats

WORKDIR /data

RUN python3 -m pip install --upgrade pip
RUN python3 -m pip install -r requirements.txt
RUN python3 -m pip install numpy --upgrade

CMD ["python3", "-u", "main.py"]