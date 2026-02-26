# Use Python 3.10 Slim (Stable and smaller than full)
FROM python:3.10-slim

# Set environment variables
# PYTHONDONTWRITEBYTECODE: Prevents Python from writing .pyc files
# PYTHONUNBUFFERED: Ensures logs are flushed immediately (vital for Docker logs)
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

# Install System Dependencies required for Prophet (C++ compilers)
# We clean up the apt cache afterwards to keep the image small
RUN apt-get update && apt-get install -y \
    build-essential \
    libgomp1 \
    && rm -rf /var/lib/apt/lists/*

# Set work directory
WORKDIR /app

# Install Python Dependencies
# We copy requirements first to leverage Docker layer caching
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the Source Code
COPY src/ ./src/

# We don't copy data files; they will be mounted via volume

# Use custom entrypoint for permissions and main command
COPY entrypoint.sh /usr/local/bin/entrypoint.sh
RUN chmod +x /usr/local/bin/entrypoint.sh

# Define build arguments for UID and GID (default to 1000 if not set)
ARG UID=1000
ARG GID=1000

# Create group and user with specified UID/GID
RUN groupadd --system --gid $GID appgroup && \
    useradd --system --uid $UID --gid appgroup appuser -d /app -s /bin/bash

# Ensure the app directory is owned by the new user
RUN chown -R appuser:appgroup /app

# Switch to non-root user
USER appuser

# Define Entrypoint and Default Command
ENTRYPOINT ["/usr/local/bin/entrypoint.sh"]
CMD ["python", "-m", "src.main"]
