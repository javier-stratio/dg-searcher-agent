#!/usr/bin/env bash

truststoreCheckStepDependencies(){
    infoLogging "Check 'TRUSTSTORE' dependencies"
    [[ -z "$STEP_SECRETS" || "$STEP_SECRETS" != true ]] && { errorLogging "Need to activate STEP_SECRETS step" 1061; exit 60; }
    truststoreCheckConfig
}

truststoreCheckConfig()
{
    infoLogging "Check 'TRUSTSTORE' environment variables"
    [[ -z "$CA_BUNDLE_PATH" ]] && { errorLogging "Need to set: CA_BUNDLE_PATH" 1061; exit 60; }
    [[ -z "$CA_BUNDLE_INSTANCE" ]] && { errorLogging "Need to set: CA_BUNDLE_INSTANCE" 1062; exit 60; }
    truststoreSetEnvironmnt
}

truststoreSetEnvironmnt()
{
    infoLogging "Retrieving 'TRUSTSTORE' from Vault with config: $VAULT_PATH/$MODULE/$CA_BUNDLE_PATH/$CA_BUNDLE_INSTANCE"
    local bundleCluster=$CA_BUNDLE_PATH
    local bundleInstance=$CA_BUNDLE_INSTANCE
    local trustStorePath="/home/governance/.truststore"
    local trustStoreName="truststore.jks"
    local trustStoreUri=$trustStorePath/$trustStoreName
    local trustStorePassword=$bundleInstance"_KEYSTORE_PASS"
    local trustStorePasswordUp=${trustStorePassword^^}
    local trustStorePasswordFinal=${trustStorePasswordUp//[.-]/_}
    getCAbundle $trustStorePath "JKS" $trustStoreName $bundleCluster $bundleInstance
    chmod -R 600 $trustStorePath
    chmod 740 $trustStorePath
    chown -R $USER:$GROUP $trustStorePath
    [[ $? -ne 0 || ! -z ${!trustStorePasswordFinal} ]] || { errorLogging "'TRUSTSTORE' wasn't retrieved from Vault" 1500; exit 50; }
    export JAVA_OPTS="$JAVA_OPTS -Djavax.net.ssl.trustStore=$trustStoreUri -Djavax.net.ssl.trustStorePassword=${!trustStorePasswordFinal}"
}

[[ -z "$STEP_TRUSTSTORE" ]] && { errorLogging "Need to set: STEP_TRUSTSTORE(boolean)" 1060; exit 60; }

[[ "$STEP_TRUSTSTORE" == true ]] && truststoreCheckStepDependencies || infoLogging "Step 'TRUSTSTORE' not configured..."

return 0