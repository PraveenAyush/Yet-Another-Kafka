FROM python:3.10-slim-buster

WORKDIR /app

COPY requirements.txt requirements.txt

RUN pip3 install -r requirements.txt

COPY ./zookeeper .

EXPOSE 9000

CMD ["python3", "__main__.py"]