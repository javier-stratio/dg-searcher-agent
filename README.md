# dg-searcher-agent

This project implements a process to take the metadata information discovered by the stratio data-governance tools to a Search-Environment hosted by the Stratio Search-Engine

It connects directly to the PostGreSQL model used by the data-gobernance core and interacts with the Search-Engine by using a REST interface

## Project structure

We have two folders where all code is located: 

### dgindexer

Here is the main code. It is based on three Akka Actors:
- ***Manager***: Control the scheduling trigger callback and the partial/total indexation protocol.
- ***Extractor***: Control the extraction of information from postgreSQL database and bulk indexer management
- ***Indexer***: Control the indexation in the Search-Engine

Execute this command in this folder:
```
mvn clean install
```
to compile this main code and its unit test. We can run compilation from root folder as well.

### testIT

This folder allocates the code of integration tests. It means, from DAO interfaces to high level tests.

To run this tests, next components must be initiated in the environment. After this, this command must be executed:

```
mvn clean install -DskipIT=false 
```

#### PostGreSQL Data Base

this can be executed this way:
- Start the first time:
```
docker run --name postgres1 -e POSTGRES_PASSWORD=stratio -e POSTGRES_USER=stratio -e POSTGRES_DB=governance -p 5432:5432 -d postgres
```
- Start next times:
```
docker start postgres1
```
- Stop, any time:
```
docker stop postgres1
```

#### Stratio Search-Engine

This project is allocated [here](https://github.com/Stratio/search-engine-core).

For developing purposes, the best idea is to run the "Completed Environment" (Verify the documentation in "local-env" folder to check if following commands are still valid):
```
cd local-env
(verify se-engine docker version in .env)
docker-compose -f docker-compose-all.yml up
```

NOTE: We will be able to run *mvn clean install* in the root folder of the project only if we have initiated this components previously. Otherwise, we will come across IT test errors.

## Jenkins
In order to run integration test in Jenkins, next configuration (after versions verification) must be included in Jenkins file:
```
    ITSERVICES = [
        ['POSTGRESQL': [
            'image': 'postgres:9.6',
            'env': [
                'POSTGRES_DB=governance',
                'POSTGRES_USER=stratio',
                'POSTGRES_PASSWORD=stratio'],
            'sleep': 20,
            'healthcheck': 5432,
        ]],
        ['ELASTICSEARCH': [
           'image': 'elasticsearch/elasticsearch:6.1.0',
           'env': [
                 'cluster.name=%%JUID',
                 'network.host=0.0.0.0',
                 'discovery.zen.minimum_master_nodes=1',
                 'action.auto_create_index=false',
                 'xpack.graph.enabled=false',
                 'xpack.ml.enabled=false',
                 'xpack.monitoring.enabled=false',
                 'xpack.security.enabled=false',
                 'xpack.watcher.enabled=false',
                 'ES_JAVA_OPTS="-Xms512m -Xmx512m"'],
           'sleep': 60,
           'healthcheck': 9200
        ]],
        ['SEMANAGER': [
         'image': 'stratio/search-engine-manager:1.0.0',
         'env': [
                'MARATHON_APP_ID=se-manager',
                'CORS_ENABLED=true',
                'ELASTICSEARCH_DEFAULT_REPLICAS=0',
                'ELASTICSEARCH_DEFAULT_REFRESH_INTERVAL=1s',
                'ELASTICSEARCH_DEFAULT_SHARDS=1',
                'ELASTICSEARCH_NODES=%%ELASTICSEARCH#0',
                'ELASTICSEARCH_SSL_ENABLED=false',
                'GZIP_ENABLED=false',
                'HTTPS_ENABLED=false',
                'JAVA_OPTS="-Xms256m -Xmx256m"',
                'LOG_LOG4J_LEVEL=INFO'],
         'sleep': 60,
         'healthcheck': 8080
        ]],
        ['SEINDEXER': [
         'image': 'stratio/search-engine-indexer:1.0.0',
         'env': [
                'MARATHON_APP_ID=se-indexer',
                'CORS_ENABLED=true',
                'ELASTICSEARCH_DEFAULT_REPLICAS=0',
                'ELASTICSEARCH_DEFAULT_REFRESH_INTERVAL=1s',
                'ELASTICSEARCH_DEFAULT_SHARDS=1',
                'ELASTICSEARCH_NODES=%%ELASTICSEARCH#0',
                'ELASTICSEARCH_SSL_ENABLED=false',
                'GZIP_ENABLED=false',
                'HTTPS_ENABLED=false',
                'JAVA_OPTS="-Xms256m -Xmx256m"',
                'LOG_LOG4J_LEVEL=INFO'],
         'sleep': 60,
         'healthcheck': 8080
        ]]
    ]

    ITPARAMETERS = """
        | -DSOURCE_CONNECTION_URL='jdbc:postgresql://%%POSTGRESQL#0:5432/governance?user=stratio&password=stratio'
        | -DSOURCE_CONNECTION_USER=stratio
        | -DSOURCE_CONNECTION_PASSWORD=stratio
        | -DSOURCE_DATABASE=governance
        | -DMANAGER_MANAGER_URL=http://%%SEMANAGER#0:8080
        | -DMANAGER_INDEXER_URL=http://%%SEINDEXER#0:8080
        | """

```
And finally set the skipIT variable in testIT/pom.xml to false.

Currently, these tests are disabled.