FROM python:3.11-slim

# Microsoft ODBC Driver 18 for SQL Server (Debian 12 / bookworm)
RUN apt-get update && apt-get install -y curl gnupg2 apt-transport-https && \
    curl -fsSL https://packages.microsoft.com/keys/microsoft.asc | \
        gpg --dearmor -o /usr/share/keyrings/microsoft-prod.gpg && \
    curl https://packages.microsoft.com/config/debian/12/prod.list > \
        /etc/apt/sources.list.d/mssql-release.list && \
    sed -i 's|https://packages.microsoft.com|[signed-by=/usr/share/keyrings/microsoft-prod.gpg] https://packages.microsoft.com|' \
        /etc/apt/sources.list.d/mssql-release.list && \
    apt-get update && \
    ACCEPT_EULA=Y apt-get install -y msodbcsql18 unixodbc-dev && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

CMD ["python3", "etl.py"]
