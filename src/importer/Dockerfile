FROM amsterdam/python
MAINTAINER datapunt@amsterdam.nl

ENV PYTHONUNBUFFERED 1

RUN mkdir -p /data
RUN chown -R datapunt /data

COPY requirements.txt /app/

WORKDIR /app

RUN pip install --no-cache-dir -r requirements.txt

COPY . /app/

USER datapunt

CMD ["bash", "import.sh"]