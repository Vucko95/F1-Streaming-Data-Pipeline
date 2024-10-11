# Use an official Python image as the base image
FROM python:3.10-slim

# Set the working directory in the container
WORKDIR /app

# Copy the current directory contents into the container
COPY . .

# Install the necessary Python dependencies
RUN pip install --no-cache-dir websockets

# Expose the ports for telemetry data and WebSocket
EXPOSE 20777/udp
EXPOSE 3001

# Set the command to run the Python script
CMD ["python", "game-rec.py"]
