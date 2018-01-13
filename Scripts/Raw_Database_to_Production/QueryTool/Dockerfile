# Use an official Python runtime as a parent image
FROM python:3

# Set the working directory to /app
WORKDIR /app

# Copy the current directory contents into the container at /app
COPY requirements.txt /app
# Install any needed packages specified in requirements.txt
RUN pip install -r requirements.txt

# Define environment variable

# Run app.py when the container launches
ENTRYPOINT ["python","/code/query-tool-db-update.py"]
CMD ["-h"]
