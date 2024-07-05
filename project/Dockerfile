FROM python:3.12

RUN apt-get update && \
    apt-get install -y wget unzip curl git sudo iputils-ping

WORKDIR /

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

RUN mkdir /project
VOLUME /project

CMD ["/bin/bash"]