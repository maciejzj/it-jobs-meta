FROM public.ecr.aws/lambda/python:3.9

COPY . ${LAMBDA_TASK_ROOT}

COPY requirements.txt  .
RUN  pip3 install -r requirements.txt --target "${LAMBDA_TASK_ROOT}"
RUN pip3 install rentry && ls -l
RUN chmod -R 777 it_jobs_meta

CMD [ "it_jobs_meta.app.lambda_handler" ]
