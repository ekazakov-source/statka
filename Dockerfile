FROM python:3.11-slim

WORKDIR /app
COPY requirements.txt /app/
RUN pip install --no-cache-dir -r requirements.txt

COPY . /app
ENV PORT=8000
EXPOSE 8000

# директория для БД (будет примонтирован внешний том)
RUN mkdir -p /data
ENV DATA_PATH=/data/data.db

CMD ["gunicorn", "-w", "1", "-k", "gthread", "-b", "0.0.0.0:8000", "main:app"]
