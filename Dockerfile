# Use an official Python runtime as a parent image
FROM python:3.9-slim

# Set the working directory in the container
WORKDIR /app

# Copy the current directory contents into the container at /app
COPY . /app

# Install any needed packages specified in requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Create a directory for the CSV file with the right permissions
RUN mkdir -p /app/data && chmod -R 777 /app/data

# Define environment variable
ENV NAME World

# Set PYTHONPATH
ENV PYTHONPATH "${PYTHONPATH}:/app"

# Run app.py when the container launches
CMD ["python", "src/app.py"]
