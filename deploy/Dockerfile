FROM python:3.13-slim

RUN apt-get update && apt-get upgrade -y
RUN apt-get install make gcc -y

WORKDIR /app
COPY ./requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

CMD ["python3.13", "main_api.py"]