FROM ubuntu:22.04 AS ubuntu

ARG DEBIAN_FRONTEND=noninteractive 

RUN apt-get update && apt-get upgrade -y && apt-get install -y apt-utils python3 python3.10-venv git

RUN mkdir app

WORKDIR /app

RUN git clone -b develop https://github.com/wmodanez/cnpj_dados_abertos.git

WORKDIR /app/cnpj_dados_abertos/

RUN python3 -m venv venv
ENV PATH="/app/cnpj_dados_abertos/venv/bin:$PATH"

RUN . venv/bin/activate
RUN pip3 install "dask[distributed]" --upgrade
RUN pip3 install --no-cache-dir -r requirements.txt 
