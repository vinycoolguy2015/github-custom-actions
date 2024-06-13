FROM python:3
COPY requirements.txt /requirements.txt
RUN pip install -r requirements.txt
COPY dynamodb_data_copy.py /dynamodb_data_copy.py
CMD ["python", "/dynamodb_data_copy.py"]
