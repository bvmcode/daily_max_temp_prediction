FROM ubuntu:latest

RUN apt-get update
RUN apt install software-properties-common -y
RUN add-apt-repository ppa:deadsnakes/ppa
RUN apt-get install -y wget libpq-dev unixodbc-dev python3.12 python3.12-distutils

RUN wget https://bootstrap.pypa.io/get-pip.py
RUN python3.12 get-pip.py

COPY ./requirements.txt /data/requirements.txt
COPY ./main.py /data/main.py
COPY ./artificats /data/artificats

WORKDIR /data

RUN python3.12 -m pip install --upgrade pip
RUN python3.12 -m pip install -r requirements.txt
RUN python3.12 -m pip install numpy --upgrade
RUN python3.12 -m pip install --ignore-installed six
RUN python3.12 -m pip install urllib3[secure]

CMD ["python3.12", "-u", "main.py"]