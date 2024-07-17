# Metamorphosis

# Key aspects learned

- Kafka Streams applications act as both a producer and a consumer within the regular Kafka world-view.

# How to run

```
docker-compose up -d
mvn spring-boot:run
```

# How to test

```
curl -X POST localhost:8080/api/v1/documents/count/vai -d "Vai de datele mele 2" | jq .
curl -X GET localhost:8080/api/v1/documents/count/vai | jq .
```

# References

Spring Kafka docs: https://spring.io/projects/spring-kafka
Reference docs: https://docs.spring.io/spring-kafka/reference/quick-tour.html

Spring Kafka samples: https://github.com/spring-projects/spring-kafka/tree/main/samples

Baeldung article: https://www.baeldung.com/spring-boot-kafka-streams
