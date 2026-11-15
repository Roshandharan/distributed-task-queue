FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1 PYTHONDONTWRITEBYTECODE=1
WORKDIR /app

COPY pyproject.toml README.md ./
COPY taskq ./taskq
RUN pip install --no-cache-dir .

COPY examples ./examples
COPY scripts ./scripts

RUN useradd --create-home --uid 10001 taskq
USER taskq

EXPOSE 8000
CMD ["python", "-m", "taskq", "worker", "--tasks", "examples.tasks"]
