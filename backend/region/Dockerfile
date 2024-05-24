FROM alpine:3
LABEL authors="squarehuang"

WORKDIR /region

COPY src/region-0.0.1-SNAPSHOT.jar /region/region.jar

EXPOSE 9090

ENTRYPOINT ["java", "-jar", "/region/region.jar"]