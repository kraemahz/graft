#!/bin/bash -ex
graft_version=$(python3 -c "import graft; print(graft.__version__)")
docker build --network host -t graft:$graft_version -f docker/Dockerfile .
docker tag graft:$graft_version graft:latest
