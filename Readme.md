running kafka and redpanda(kafka wub ui)
```cmd
docker-compose -f kafka/docker-compose.yml up -d
```

try to consume message from kafka but cache message on memory and commit message (aka. batch consume)