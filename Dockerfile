FROM python:2.7.15

RUN dd if=/dev/urandom of=/tmp/file1.rnd ibs=1024k count=10

COPY . /app

WORKDIR /app

RUN pip install -r requirements.txt

CMD python setup.py test
