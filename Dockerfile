# Dockerfile, Image, Container

FROM python:3.10.4

ADD ./lib/data_prep .

ADD ./creds/service_account.json .

ADD requirements_docker.txt .

ADD .env .

RUN pip install -r ./requirements_docker.txt

CMD ["python", "daily_update_docker_version.py"]