FROM python:3.8-slim-buster

WORKDIR /app

ENV TWITTER_BEARER_TOKEN=YOUR_TWITTER_API_BEARER_TOKEN


COPY requirements.txt requirements.txt
RUN pip3 install --no-cache-dir -r requirements.txt

COPY . .

CMD [ "python3" , "./main.py"]
