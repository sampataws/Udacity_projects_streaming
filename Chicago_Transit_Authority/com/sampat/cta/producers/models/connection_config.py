from typing import Dict


class Connections:
    KAFKA_BROKER: str = "PLAINTEXT://kafka0:9092"
    REST_PROXY: str = "http://rest-proxy:8082"
    SCHEMA_REGISTRY: str = "http://schema-registry:8081"
    CONNECT: str = "http://kafka-connect:8083"
    KSQL: str = "http://ksql:8088"
    POSTGRES: Dict[str, str] = {
        "connection.url": "jdbc:postgresql://postgres:5432/cta",
        "user": "cta_admin",
        "password": "chicago",
    }