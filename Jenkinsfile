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

    DEV = {
        config ->

            doCompile(config)

            doIT(config)

            doPackage(config)

            parallel(DEPLOY: {doDeploy(config)},
                     DOCKER: {doDocker(config)},
                     failFast: config.FAILFAST)
    }
}
