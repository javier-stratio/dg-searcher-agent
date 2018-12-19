#!/usr/bin/env bash

createDirectory(){
    mkdir -p $1 && chmod -R 0740 $1 && chown $USER:$GROUP $1
}

secretsCheckStepDependencies(){
    infoLogging "Check 'SECRETS' dependencies"
    secretsCheckConfig
}

secretsCheckConfig()
{
    infoLogging "Check 'SECRETS' environment variables..."
    [[ -z "$VAULT_HOSTS" ]] && { errorLogging "Need to set: VAULT_HOSTS" 1021; exit 20; }
    [[ -z "$VAULT_PORT" ]] && { errorLogging "Need to set: VAULT_PORT" 1022; exit 20; }
    [[ -z "$VAULT_PATH" ]] && { errorLogging "Need to set: VAULT_PATH" 1023; exit 20; }
    secretsSetEnvironment
}

secretsSetEnvironment()
{
    infoLogging "Creating 'SECRETS' directories in: /home/$USER"
    [[ -d "/home/$USER/.postgresql" ]] || createDirectory "/home/$USER/.postgresql"
    [[ -d "/home/$USER/.tls" ]] || createDirectory "/home/$USER/.tls"
    [[ -d "/home/$USER/.truststore" ]] || createDirectory "/home/$USER/.truststore"
    [[ -z "$VAULT_TOKEN" ]] && { login; infoLogging "Dynamic authentication enabled";}
}

[[ -z "$STEP_SECRETS" ]] && { errorLogging "Need to set: STEP_SECRETS(boolean)" 1020; exit 20; }

[[ "$STEP_SECRETS" == true ]] && secretsCheckStepDependencies || infoLogging "Step 'SECRETS' not configured..."

return 0