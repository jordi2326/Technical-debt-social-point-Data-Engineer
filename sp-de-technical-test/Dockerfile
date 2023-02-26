FROM postgres:latest

# Python 3 installation
RUN apt-get update && apt-get install -y python3 python3-pip

RUN mkdir /code
WORKDIR /code
COPY requirements.txt /code/
RUN pip3 install -r requirements.txt
COPY . /code/