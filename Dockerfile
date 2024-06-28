FROM python:3.9-slim

WORKDIR /taxi_etl

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

ENV PYTHONPATH=/taxi_etl:$PYTHONPATH

CMD ["python", "etl/main.py"]