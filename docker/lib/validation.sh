#!/usr/bin/env bash

checkBoolean(){
    [[ "$1" == true ]] || [[ "$1" == false ]]
}

checkUri(){
    local urlRegex='(https?|ftp|file)://[-A-Za-z0-9\+&@#/%?=~_|!:,.;]*[-A-Za-z0-9\+&@#/%=~_|]'
    [[ $1 =~ $regex ]]
}

checkUriJDBC(){
    local urlRegex='jdbc:postgresql://[-A-Za-z0-9\+&@#/%?=~_|!:,.;]*[-A-Za-z0-9\+&@#/%=~_|]'
    [[ $1 =~ $regex ]]
}

