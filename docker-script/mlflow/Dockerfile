FROM ubuntu:22.04

COPY apt_packages.txt .
RUN apt-get update && xargs apt-get install -y < apt_packages.txt

COPY pip_packages.txt .
RUN xargs pip install < pip_packages.txt
