#!/usr/bin/env bash

export TEMPLATE_CLUSTER="${TEMPLATE_CLUSTER:-megadev.labs.stratio.com}"

infoLogging "Set '$TEMPLATE_CLUSTER' environment..."

export STEP_SECRETS="${STEP_SECRETS:-true}"
export VAULT_HOSTS="${VAULT_HOSTS:-vault.service.paas.labs.stratio.com}"
export VAULT_PORT=${VAULT_PORT:-8200}
export VAULT_PATH="${VAULT_PATH:-userland}"

export STEP_OAUTH2="${STEP_OAUTH2:-true}"
export SECURITY_OAUTH2_CLIENT_ACCESS_TOKEN_URI="https://$TEMPLATE_CLUSTER:9005/sso/oauth2.0/accessToken"
export SECURITY_OAUTH2_CLIENT_USER_AUTHORIZATION_URI="https://$TEMPLATE_CLUSTER:9005/sso/oauth2.0/authorize"
export SECURITY_OAUTH2_CLIENT_CLIENT_AUTHENTICATION_SCHEME="form"
export SECURITY_OAUTH2_RESOURCE_USER_INFO_URI="https://$TEMPLATE_CLUSTER:9005/sso/oauth2.0/profile"
export SECURITY_OAUTH2_RESOURCE_PREFER_TOKEN_INFO=false

export STEP_POSTGRES="${STEP_POSTGRES:-TLS}"
export SPRING_DATASOURCE_URL="${SPRING_DATASOURCE_URL:-jdbc:postgresql://pg-0001.postgrestls.mesos:5432,pg-0002.postgrestls.mesos:5432,pg-0003.postgrestls.mesos:5432/governance?targetServerType=master}"
export DB_CERT_PATH=${DB_CERT_PATH:="dg-bootstrap"}
export DB_CERT_FQDN=${DB_CERT_FQDN:="dg-bootstrap"}
export DB_CA_PATH=${DB_CA_PATH:="ca-trust"}
export DB_CA_INSTANCE=${DB_CA_INSTANCE:="default"}

export STEP_TLS="${STEP_TLS:-true}"
export TLS_CERT_PATH="${TLS_CERT_PATH:-dg-businessglossary-api}"
export TLS_CERT_FQDN="${TLS_CERT_FQDN:-dg-businessglossary-api}"

export STEP_TRUSTSTORE="${STEP_TRUSTSTORE:-true}"
export CA_BUNDLE_PATH="${CA_BUNDLE_PATH:=ca-trust}"
export CA_BUNDLE_INSTANCE="${CA_BUNDLE_INSTANCE:=default}"
