FROM python:3.13-slim
COPY --from=ghcr.io/astral-sh/uv:0.13.7 /uv /bin/
WORKDIR /app
COPY pyproject.toml uv.lock ./
RUN uv sync --locked --no-dev --no-install-project
ENV PATH="/app/.venv/bin:$PATH" PYTHONUNBUFFERED=1
ARG SERVICE
COPY $SERVICE .
CMD ["python", "main.py"]
