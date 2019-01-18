@Library('libpipelines@master') _

hose {
    MAIL = 'governance'
    LANG = 'scala'
    SLACKTEAM = 'data-governance'
    MODULE = 'dg-searcher-agent'
    REPOSITORY = 'dg-searcher-agent'
    DEVTIMEOUT = 30
    RELEASETIMEOUT = 30
    MAXITRETRIES = 2
    BUILDTOOLVERSION = '3.5.0'
    NEW_VERSIONING = 'true'
    KMS_UTILS = '0.4.0'

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

    DEV = {
        config ->

            doCompile(config)

            doIT(config)

            doPackage(config)

            parallel(QC: {doStaticAnalysis(config)},
                     DEPLOY: {doDeploy(config)},
                     DOCKER: {doDocker(config)},
                     failFast: config.FAILFAST)
    }
}
