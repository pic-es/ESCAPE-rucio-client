#!/bin/bash
docker run --env-file env --rm -p 8888:8888 -e RUCIO_CFG_ACCOUNT=bruzzese -v "/$(pwd)/user-certs/usercert.pem:/home/jovyan/client.crt" -v "/$(pwd)/user-certs/newkey.pem:/home/jovyan/client.key" --name=rucio-jhub rucio-client-jhub:latest
