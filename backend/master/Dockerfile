FROM amazoncorretto:17.0.11-al2023-headless

WORKDIR /master

COPY build/libs/master-0.0.1-SNAPSHOT.jar /master/master.jar

EXPOSE 8080

ENTRYPOINT ["java", "-jar", "/master/master.jar"]