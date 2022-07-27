FROM python:3.13-slim@sha256:3dd7cc108ec1493442514f5c2a871af6af0ec31d768ff6e378a93340c3b3db5f
WORKDIR /app
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1 PIP_DISABLE_PIP_VERSION_CHECK=1
RUN groupadd --gid 10001 relay && useradd --uid 10001 --gid relay --no-create-home --no-log-init relay && mkdir /state && chown relay:relay /state
COPY requirements.lock ./requirements.lock
RUN python -m pip install --no-cache-dir --require-hashes -r requirements.lock
COPY app ./app
USER 10001:10001
EXPOSE 8000
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000", "--workers", "1", "--no-server-header"]
