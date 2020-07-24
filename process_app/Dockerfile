FROM python:3.6
RUN mkdir /app
WORKDIR /app
ADD requirements.txt /app/requirements.txt
RUN pip install -r /app/requirements.txt
ADD * /app/


ENTRYPOINT ["python","/app/app.py"]