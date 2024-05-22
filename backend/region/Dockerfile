FROM alpine:3
LABEL authors="squarehuang"

WORKDIR /region

COPY src/region.jar /region/region.jar

EXPOSE 9090

ENTRYPOINT ["java", "-jar", "/region/region.jar"]