set -eo pipefail

PLATFORM="deb-x64" # must be in "deb-x64", "deb-i386", "rpm-x64", "rpm-i386"
AGENT_BRANCH="master" # Branch of dd-agent repo to use, default "master"
OMNIBUS_BRANCH="master" # Branch of dd-agent-omnibus repo to use, default "master"
AGENT_VERSION="5.9.1-mk" # default to the latest tag on that branch
LOG_LEVEL="info" # default to "info"
LOCAL_AGENT_REPO="$(pwd)" # Path to a local repo of the agent to build from. Defaut is not set and the build will be done against the github repo

mkdir -p pkg
docker run --rm --name "dd-agent-build-$PLATFORM" \
  -e OMNIBUS_BRANCH=$OMNIBUS_BRANCH \
  -e LOG_LEVEL=$LOG_LEVEL \
  -e AGENT_BRANCH=$AGENT_BRANCH \
  -e AGENT_VERSION=$AGENT_VERSION \
  -e LOCAL_AGENT_REPO=/dd-agent-repo \
  -v "`pwd`/pkg:/dd-agent-omnibus/pkg" \
  -v /tmp/keys:/keys \
  -v "/tmp/cache/$PLATFORM:/var/cache/omnibus" \
  -v $LOCAL_AGENT_REPO:/dd-agent-repo:ro \
  "datadog/docker-dd-agent-build-$PLATFORM"

BUILD_VERSION=$(jq -r .build_version pkg/version-manifest.json)
docker build -t $DOCKER_REGISTRY/dd-agent:$BUILD_VERSION .
docker push $DOCKER_REGISTRY/dd-agent:$BUILD_VERSION
