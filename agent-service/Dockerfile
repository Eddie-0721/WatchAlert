FROM python:3.12-slim

WORKDIR /app

# The default mirror is reachable from mainland China. It can be overridden by
# the CI build command when a company mirror is required.
ARG PIP_INDEX_URL=https://pypi.tuna.tsinghua.edu.cn/simple
ENV PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

COPY requirements.txt .
RUN pip install --no-cache-dir --index-url=${PIP_INDEX_URL} -r requirements.txt

COPY app ./app
EXPOSE 8080

CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8080"]
