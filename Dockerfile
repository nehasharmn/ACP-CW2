# Build stage
FROM maven:3.9.9-amazoncorretto-21-debian AS build
WORKDIR /app
COPY pom.xml .
COPY src ./src
RUN mvn clean package -DskipTests

# Run stage
FROM openjdk:21
WORKDIR /app
COPY --from=build /app/target/acp_submission_2*.jar app.jar
EXPOSE 8080

CMD ["java", "-jar", "app.jar"]