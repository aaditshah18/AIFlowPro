# Use the official Python image from the Docker Hub
FROM python:3.9

# Set the working directory in the container
WORKDIR /app

# Copy the requirements file to the working directory
COPY modeling/requirements.txt .

# Install any needed packages specified in requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Copy the entire modeling directory contents into the container at /app
COPY modeling /app/modeling

# Set the working directory to /app/modeling
WORKDIR /app/modeling


# Command to run on container start
CMD ["sh", "-c", "python preprocessing-cleaning.py && python modeling/xgboost-model-v2.py"]
