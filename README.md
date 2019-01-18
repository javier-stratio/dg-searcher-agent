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
to compile this main code and its unit test

### testIT

This folder allocates the code of integration tests. It means, from DAO interfaces to high level tests.

To run this tests, next components must be initiated in the environment:

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
mvn clean install -Dmaven.test.skip=true
cd local-env
docker-compose -f docker-compose-all.yml up
```

NOTE: We will be able to run *mvn clean install* in the root folder of the project only if we have initiated this components previously. Otherwise, we will come across IT test errors.