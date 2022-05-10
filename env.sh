#!/bin/sh
export X_CSI_UNITY_ENDPOINT=https://10.247.13.94
export X_CSI_UNITY_USER=admin
export X_CSI_UNITY_PASSWORD=Password123!
export X_CSI_UNITY_INSECURE=true
export X_CSI_UNITY_NODENAME=master-1-Of3RJM3xw0ND2.domain
export X_CSI_PRIVATE_MOUNT_DIR=/root/JenkinTests/privmount
export X_CSI_STAGING_TARGET_PATH=/root/JenkinTests/stagemountpath
export X_CSI_PUBLISH_TARGET_PATH=/root/JenkinTests/publishmountpath
export CSI_ENDPOINT=/home/bin/csi.sock
export X_CSI_DEBUG=true
export STORAGE_POOL=pool_1
export NAS_SERVER="nas_1"
export X_CSI_REQ_LOGGING="true"
export X_CSI_REP_LOGGING="true"
export GOUNITY_DEBUG="true"
export X_CSI_UNITY_AUTOPROBE="true"
export DRIVER_NAME="csi-unity.dellemc.com"
export DRIVER_CONFIG=/root/csi-unity/values.yaml
export DRIVER_SECRET=/root/csi-unity/secret.yaml
export X_CSI_UNITY_ARRAYID="APM00202802812"
export CSI_LOG_LEVEL="info"
export ALLOW_RWO_MULTIPOD_ACCESS="true"
export MAX_UNITY_VOLUMES_PER_NODE=0
export SYNC_NODE_INFO_TIME_INTERVAL=15
export TENANT_NAME=""

