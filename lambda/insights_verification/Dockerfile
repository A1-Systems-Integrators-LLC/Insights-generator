FROM public.ecr.aws/lambda/python:3.9.2022.04.27.10-x86_64

COPY verify_insights.py /asset/
COPY requirements.txt /tmp/
RUN pip3 install -r /tmp/requirements.txt -t /asset