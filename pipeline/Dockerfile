FROM python:3.13.11-slim

WORKDIR /code
ENV PATH="/code/.venv/bin:$PATH"

RUN pip install pandas pyarrow

COPY pipeline.py /code/
COPY pyproject.toml /code/
COPY uv.lock /code/

ENTRYPOINT ["python", "pipeline.py"]
