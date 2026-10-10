FROM python:3.11-slim

WORKDIR /app

ENV PYTHONUNBUFFERED=1

# Commander, FastMCP & HTTP-Server installieren
RUN pip install --no-cache-dir keepercommander fastmcp uvicorn starlette

COPY keeper-mcp-server.py /app/server.py

EXPOSE 8000

CMD ["python", "/app/server.py"]

