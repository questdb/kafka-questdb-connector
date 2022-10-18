FROM maven:3.8-jdk-11-slim AS builder
COPY ./pom.xml /opt/stocks/pom.xml
COPY ./src ./opt/stocks/src
WORKDIR /opt/stocks
RUN mvn clean install -DskipTests

FROM azul/zulu-openjdk:11-latest
COPY --from=builder /opt/stocks/target/kafka-samples-stocks-*.jar /stocks.jar
CMD ["java", "-jar", "/stocks.jar"]