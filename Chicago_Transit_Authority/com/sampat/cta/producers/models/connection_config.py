from typing import Dict


class Connections:
    KAFKA_BROKER: str = "PLAINTEXT://localhost:9092"
    REST_PROXY: str = "http://localhost:8082"
    SCHEMA_REGISTRY: str = "http://localhost:8081"
    CONNECT: str = "http://localhost:8083"
    KSQL: str = "http://localhost:8088"
    POSTGRES: Dict[str, str] = {
        "connection.url": "jdbc:postgresql://postgres:5432/cta",
        "user": "cta_admin",
        "password": "chicago",
    }