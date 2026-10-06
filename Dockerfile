FROM python:3.11.17-slim

COPY --from=ghcr.io/astral-sh/uv:0.12.23 /uv /bin/uv

WORKDIR /app

COPY pyproject.toml uv.lock ./
RUN uv sync --locked --no-dev --no-install-project

COPY src/ src/
RUN uv sync --locked --no-dev

RUN useradd --system --no-create-home fda
USER fda

ENV PATH=/app/.venv/bin:$PATH
