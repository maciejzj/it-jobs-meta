FROM python:3.9-bullseye

RUN python -m pip install --upgrade pip

RUN useradd -m itjm-user
USER itjm-user
WORKDIR /home/itjm-user

ENV PATH="/home/itjm-user/.local/bin:${PATH}"

COPY --chown=itjm-user:itjm-user . it-jobs-meta

RUN cd it-jobs-meta && \
    python -m pip install build && \
    python -m build && \
    pip install --user dist/it_jobs_meta*.whl
