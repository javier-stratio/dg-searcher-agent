#!/usr/bin/env bash

postgresMD5CheckStepDependencies(){
    infoLogging "Check 'POSTGRESMD5' dependencies"
    [[ -z "$STEP_SECRETS" || "$STEP_SECRETS" != true ]] && { errorLogging "Need to activate STEP_SECRETS step" 1061; exit 60; }
    postgresMD5CheckConfig
}

postgresTLSCheckStepDependencies(){
    infoLogging "Check 'POSTGRESTLS' dependencies"
    [[ -z "$STEP_SECRETS" || "$STEP_SECRETS" != true ]] && { errorLogging "Need to activate STEP_SECRETS step" 1061; exit 60; }
    postgresTLSCheckConfig
}

postgresMD5CheckConfig(){
    infoLogging "Check 'POSTGRESMD5' environment variables"
    postgresMD5SetEnvironment
}

postgresMD5SetEnvironment(){
    infoLogging "Retrieving 'POSTGRESMD5' user/pass from Vault with config: $VAULT_PATH/$DB_MD5_USER_PATH/postgres"
    local userPostgres=$DB_MD5_USER_PATH"_POSTGRES_USER"
    local passPostgres=$DB_MD5_USER_PATH"_POSTGRES_PASS"
    local userPostgresUp=${userPostgres^^}
    local passPostgresUp=${passPostgres^^}
    local userPostgresFinal=${userPostgresUp//[.-]/_}
    local passPostgresFinal=${passPostgresUp//[.-]/_}
    getPass $VAULT_PATH $DB_MD5_USER_PATH "postgres"
    [[ $? -ne 0 || ! -z ${!userPostgresFinal} || ! -z ${!passPostgresFinal} ]] || { errorLogging "'POSTGRESMD5' User wasn't retrieved from Vault" 1500; exit 50; }
    export SPRING_DATASOURCE_USERNAME=${!userPostgresFinal}
    export SPRING_DATASOURCE_PASSWORD=${!passPostgresFinal}
}

postgresTLSCheckConfig()
{
    infoLogging "Check 'POSTGRESTLS' environment variables"
    [[ -z "$DB_CERT_PATH" ]] && { errorLogging "Need to set: DB_CERT_PATH" 1041; exit 30; }
    [[ -z "$DB_CERT_FQDN" ]] && { errorLogging "Need to set: DB_CERT_FQDN" 1042; exit 30; }
    [[ -z "$DB_CA_PATH" ]] && { errorLogging "Need to set: DB_CA_PATH" 1043; exit 30; }
    [[ -z "$DB_CA_INSTANCE" ]] && { errorLogging "Need to set: DB_CA_INSTANCE" 1044; exit 30; }
    postgresTLSSetEnvironment
}

postgresTLSSetEnvironment(){
    infoLogging "Retrieving 'POSTGRESTLS' certificates from Vault with config: $VAULT_PATH/$MODULE/$DB_CERT_PATH/$DB_CERT_FQDN"
    local sslInstance=$DB_CERT_PATH
    local sslFqdn=$DB_CERT_FQDN
    local caBundleCluster=$DB_CA_PATH
    local caBundleInstance=$DB_CA_INSTANCE
    local certsLocation="/home/governance/.postgresql"
    local sslPemCert="$certsLocation/$sslFqdn.pem"
    local sslPemKey="$certsLocation/$sslFqdn.key"
    local sslKey="$certsLocation/$sslFqdn.pk8"
    local sslRootCert="$certsLocation/root.pem"
    getCert $VAULT_PATH $sslInstance $sslFqdn "PEM" $certsLocation
    getCAbundle $certsLocation "PEM" "root.pem" $caBundleCluster $caBundleInstance
    openssl pkcs8 -topk8 -inform pem -outform der -in $sslPemKey -out $sslKey -nocrypt
    chmod -R 600 $certsLocation
    chmod 740 $certsLocation
    chown -R $USER:$GROUP $certsLocation
    [[ $? -ne 0 || -f $sslPemCert || -f $sslKey || -f $sslRootCert ]] || { errorLogging "'POSTGRESTLS' Certificates wasn't retrieved from Vault" 1045; exit 50; }
    [[ "$DATASOURCE_URL" = *"?"* ]] && export DATASOURCE_URL="$DATASOURCE_URL&" || export DATASOURCE_URL="$DATASOURCE_URL?"
    export SOURCE_CONNECTION_URL="$DATASOURCE_URL?ssl=true&sslmode=verify-full&user=$sslFqdn&password=&sslcert=$sslPemCert&sslkey=$sslKey&sslrootcert=$sslRootCert"
}

[[ -z "$STEP_POSTGRES" ]] && { errorLogging "Need to set: STEP_POSTGRES(TLS|MD5)" 1040; exit 40; }
[[ -z "$DATASOURCE_URL" ]] && { errorLogging "Need to set: DATASOURCE_URL" 1040; exit 40; }

[[ "$STEP_POSTGRES" == "TLS" ]] && postgresTLSCheckStepDependencies || infoLogging "Step 'POSTGRESTLS' security not configured..."
[[ "$STEP_POSTGRES" == "MD5" ]] && postgresMD5CheckStepDependencies || infoLogging "Step 'POSTGRESMD5' security not configured..."

return 0

