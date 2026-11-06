FROM eclipse-temurin:26-jdk AS build
WORKDIR /src
COPY .mvn .mvn
COPY mvnw pom.xml ./
COPY src src
RUN ./mvnw -q -DskipTests package

FROM eclipse-temurin:26-jre
WORKDIR /app
COPY --from=build /src/target/financial-ledger-0.0.1-SNAPSHOT.jar app.jar
EXPOSE 8080
ENTRYPOINT ["java", "-jar", "app.jar"]
