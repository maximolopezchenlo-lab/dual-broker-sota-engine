# Use official Python runtime as a parent image
FROM python:3.10-slim

# Set environment variables
ENV PYTHONUNBUFFERED=1 \
    PORT=7860 \
    NODE_ENV=production \
    MCP_SIMULATION_MODE=true

# Set working directory
WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Copy python dependencies list first
COPY core_agents/requirements.txt ./core_agents/requirements.txt

# Install python packages
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r core_agents/requirements.txt && \
    pip install --no-cache-dir matplotlib

# Copy the rest of the application code
COPY . .

# Expose port 7860 (Hugging Face Spaces requirement)
EXPOSE 7860

# Run the live trading loop server
CMD ["python", "run_live_trading_loop.py"]
