FROM jupyter/scipy-notebook:latest

USER root

COPY apt_packages.txt .

# Ubuntuの各種パッケージとtalibをインストールする (apt_packages.txtは改行コードLFでないと動作しないことに注意)
RUN apt-get -y update && xargs apt-get -y install < apt_packages.txt
RUN wget http://prdownloads.sourceforge.net/ta-lib/ta-lib-0.4.0-src.tar.gz && \
    tar -zxvf ta-lib-0.4.0-src.tar.gz && \
    cd ta-lib && \
    ./configure --prefix=/usr && \
    make && \
    make install && \
    cd ../ && \
    rm -rf ta-lib-0.4.0-src.tar.gz && \
    rm -rf ta-lib

USER jovyan
WORKDIR /home/jovyan/work

# Pythonの各種パッケージをインストールする (pip_packages.txtは改行コードLFでないと動作しないことに注意)
COPY pip_packages.txt .
RUN xargs pip --no-cache-dir install < pip_packages.txt

# 必要に応じて自分のコードをcloneする (ここではこのテンプレートそのものをcloneしている)
RUN git config --global user.email "your_email_address@.com" && \
    git config --global user.name "your_name" && \
    git clone https://github.com/WannabeBotter/crypto-wannabebotter-template.git
