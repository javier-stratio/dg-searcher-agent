# DG-Indexer integration Test

In this project, integration between application interfaces and external technologies will be tested. Such external technologies are next:

- Stratio Search Engine
- Postgres SQL Data Base

## How to start external technologies


```
docker run -dit -e POSTGRES_PASSWORD=pass -e POSTGRES_USER=user -e POSTGRES_DB=governance -p 5432:5432 postgres
```


## How to run a postgres SQL shell

```
DOCKER_ID=$(docker ps | grep postgres | awk '{print $1}')

docker exec -it $DOCKER_ID psql -d governance -U user -W
```

