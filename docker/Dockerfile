FROM python:3.7.0

WORKDIR /workspace

# インストール
RUN apt-get update && apt-get install
RUN apt install unzip
# RUN apt-get install gcc
# RUN apt-get install build-essential

# python環境
COPY requirements.txt /workspace/
RUN pip install --upgrade pip \ 
    pip install -r requirements.txt