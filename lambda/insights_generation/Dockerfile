FROM public.ecr.aws/lambda/python:3.9.2022.04.27.10-x86_64

COPY main.py /asset/
COPY examples.txt /asset/
COPY insights_generation_prompt.txt /asset/
COPY insights_paraphrasing_prompt.txt /asset/
COPY insights_verification_prompt.txt /asset/
COPY generate_insights.py /asset/
COPY paraphrase_insights.py /asset/
COPY requirements.txt /tmp/
RUN pip3 install -r /tmp/requirements.txt -t /asset