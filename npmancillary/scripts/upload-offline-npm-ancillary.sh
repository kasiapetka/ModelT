#!/bin/sh
# Upload Offline NPM Ancillary Module
# Supports: Linux or Mac
# 1. Downloads the npm-ancillary-module artifact from either the release repository or the snapshot repository.
# 2. Performs "npm install on the npm resource folder"
# 3. Zips the module and uploads to artifactory (release or snapshot)

OS_NAME=$(uname -s)

REPOSITORY_ID="hybris-repository"

RELEASE_REPO="https://repository.hybris.com/hybris-release"
SNAPSHOT_REPO="https://repository.hybris.com/hybris-snapshot"

PROJECT_GROUPID="de.hybris.platform"
PROJECT_ARTIFACTID="npm-ancillary-module"

ARTIFACT_VERSION=$1

# option to download of the artifact (useful when running from CI that doesn't have permission to hybris-snapshot for example).
DOWNLOAD_ARTIFACT=$2

if [[ "${ARTIFACT_VERSION}" == "" ]] ; then
    echo "Local usage: ./upload-offline-npm-ancillary.sh ARTIFACT_VERSION true"
    echo "Example local usage: ./upload-offline-npm-ancillary.sh 6.6.0.0-RC4-SNAPSHOT true"

    echo "To disable the artifact download: ./upload-offline-npm-ancillary.sh ARTIFACT_VERSION false"
    exit -1
fi

if [ "${DOWNLOAD_ARTIFACT}" == "true" ] ; then
    WORKSPACE=$(pwd)/build
else
    WORKSPACE=$(pwd)
fi

NPM_RESOURCE_HOME=${WORKSPACE}/hybris/bin/ext-content/npmancillary/resources/npm

if [[ "${ARTIFACT_VERSION}" == *SNAPSHOT ]] ; then
    TARGET_REPOSITORY=$SNAPSHOT_REPO
else
    TARGET_REPOSITORY=$RELEASE_REPO
fi

if [ "${OS_NAME}" = "Darwin" ] ; then
    NODE_HOME=${NPM_RESOURCE_HOME}/node/node-v10.7.0-darwin-x64/bin
    OFFLINE_PROJECT_ARTIFACT_ID=offline-darwin-${PROJECT_ARTIFACTID}
elif [ "${OS_NAME}" = "Linux" ] ; then
    NODE_HOME=${NPM_RESOURCE_HOME}/node/node-v10.7.0-linux-x64/bin
    OFFLINE_PROJECT_ARTIFACT_ID=offline-linux-${PROJECT_ARTIFACTID}
fi

echo """
Running upload-offline-npm-ancillary.sh

OS_NAME: ${OS_NAME}
WORKSPACE: ${WORKSPACE}

REPOSITORY_ID: ${REPOSITORY_ID}

RELEASE_REPO: ${RELEASE_REPO}
SNAPSHOT_REPO: ${SNAPSHOT_REPO}

PROJECT_GROUPID: ${PROJECT_GROUPID}
PROJECT_ARTIFACTID: ${PROJECT_ARTIFACTID}
ARTIFACT_VERSION: ${ARTIFACT_VERSION}

NPM_RESOURCE_HOME: ${NPM_RESOURCE_HOME}

TARGET_REPOSITORY: ${TARGET_REPOSITORY}

NODE_HOME: ${NODE_HOME}
OFFLINE_PROJECT_ARTIFACT_ID: ${OFFLINE_PROJECT_ARTIFACT_ID}
"""

if [ "${DOWNLOAD_ARTIFACT}" == "true" ] ; then
    # Create workspace and download artifact
    rm -rf $WORKSPACE
    mkdir -p $WORKSPACE
    cd $WORKSPACE
    echo "Downloading artifact with dest=${WORKSPACE}/${PROJECT_ARTIFACTID}-${ARTIFACT_VERSION}.zip"
    mvn org.apache.maven.plugins:maven-dependency-plugin:2.4:get \
       -Dartifact=${PROJECT_GROUPID}:${PROJECT_ARTIFACTID}:${ARTIFACT_VERSION}:zip \
       -Ddest=${WORKSPACE}/${PROJECT_ARTIFACTID}-${ARTIFACT_VERSION}.zip
    echo "Unziping artifact"
    unzip -qq ${PROJECT_ARTIFACTID}-${ARTIFACT_VERSION}.zip
fi

echo "Updating PATH for node binary to $NODE_HOME"
export PATH=$NODE_HOME:$PATH

cd ${NPM_RESOURCE_HOME}
echo "Running npm install"
npm install

cd $WORKSPACE
# zip file might now always exist.
rm -f ${PROJECT_ARTIFACTID}-${ARTIFACT_VERSION}.zip

echo "Zipping offline ancillary"
# need to store symbolic links as such in the zip
zip -ryq ${OFFLINE_PROJECT_ARTIFACT_ID}-${ARTIFACT_VERSION}.zip .

echo "Deploying artifact ${OFFLINE_PROJECT_ARTIFACT_ID}-${ARTIFACT_VERSION}.zip"
mvn deploy:deploy-file -DrepositoryId=hybris-repository -Durl=${TARGET_REPOSITORY} -Dfile=${OFFLINE_PROJECT_ARTIFACT_ID}-${ARTIFACT_VERSION}.zip -DgroupId=${PROJECT_GROUPID} -Dversion=${ARTIFACT_VERSION} -DartifactId=${OFFLINE_PROJECT_ARTIFACT_ID} -DgeneratePom=true

echo "Cleaning workspace"
if [ "${DOWNLOAD_ARTIFACT}" == "true" ] ; then
    rm -rf $WORKSPACE
else
    rm ${OFFLINE_PROJECT_ARTIFACT_ID}-${ARTIFACT_VERSION}.zip
fi