FROM ubuntu:20.04

MAINTAINER Kamrul Hasan "hasan.alive@gmail.com"

RUN apt-get update -y
RUN apt-get install -y python3-pip python-dev build-essential

# We copy just the requirements.txt first to leverage Docker cache
COPY . /app
WORKDIR /app
RUN pip3 install -r requirements.txt
ENTRYPOINT [ "python3" ]

CMD ["app.py","unit_test.py","/app/config_restapi.json" ]
