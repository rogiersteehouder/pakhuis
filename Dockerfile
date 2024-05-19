FROM python:3.12-alpine

WORKDIR /server

COPY requirements.txt .
COPY pakhuis/* ./pakhuis/

RUN python -m pip install --compile -r requirements.txt

VOLUME ["/server/instance/", "/server/log/"]

EXPOSE 80

ENTRYPOINT ["python", "-m", "pakhuis"]
CMD ["--cfg", "./instance/config.toml", "--loglevel", "info", "--log-dir", "./log/"]
