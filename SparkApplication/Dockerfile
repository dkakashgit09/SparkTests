# Use an official OpenJDK runtime as a base image
FROM openjdk:11-jdk-slim

# Set the working directory in the container
WORKDIR /app

# Copy the packaged JAR file into the container at /app
COPY target/SparkApplication-0.0.1-SNAPSHOT.jar /app/SparkApplication-0.0.1-SNAPSHOT.jar

# Make port 8080 available to the world outside this container
EXPOSE 8080

# Run the JAR file
CMD ["java", "-jar", "/app/SparkApplication-0.0.1-SNAPSHOT.jar"]