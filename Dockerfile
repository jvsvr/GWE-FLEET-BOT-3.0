FROM python:3.11-slim

WORKDIR /app

# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy bot
COPY bot.py .

# Create data directory for SQLite
RUN mkdir -p /app/data

CMD ["python", "-u", "bot.py"]
