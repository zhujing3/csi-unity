package service

import (
	"context"
	"strings"

	"github.com/dell/gounity/types"

	csiext "github.com/dell/dell-csi-extensions/replication"
	"github.com/dell/gounity"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func (s *service) CreateRemoteVolume(ctx context.Context, req *csiext.CreateRemoteVolumeRequest) (*csiext.CreateRemoteVolumeResponse, error) {
	ctx, log, _ := GetRunidLog(ctx)
	log.Debugf("Executing CreateRemoteVolume with args: %s", req.String())
	volID := req.GetVolumeHandle()
	if volID == "" {
		return nil, status.Error(codes.InvalidArgument, "volume ID is required")
	}

	volID, protocol, arrayID, unity, err := s.validateAndGetResourceDetails(ctx, volID, volumeType)
	if err != nil {
		return nil, err
	}
	ctx, log = setArrayIDContext(ctx, arrayID)
	if err := s.requireProbe(ctx, arrayID); err != nil {
		return nil, err
	}
	//remoteVolumeResp := &csiext.CreateRemoteVolumeResponse{}
	if protocol == NFS {
		fsAPI := gounity.NewFilesystem(unity)
		fileSystems, err := fsAPI.FindFilesystemByID(ctx, volID)
		if err != nil {
			return nil, err
		}
		replAPI := gounity.NewReplicationSession(unity)
		rs, err := replAPI.FindReplicationSessionBySrcResourceID(ctx, fileSystems.FileContent.StorageResource.ID)
		if err != nil {
			return nil, err
		}

		var remoteVolumeID string
		replSession, err := replAPI.FindReplicationSessionByID(ctx, rs.ReplicationSessionContent.ReplicationSessionID)
		if err != nil {
			return nil, err
		}
		remoteVolumeID = replSession.ReplicationSessionContent.DstResourceID
		if remoteVolumeID == "" {
			return nil, status.Errorf(codes.Internal, "couldn't find volume id in replication session")
		}
		remoteArrayID := req.Parameters[s.WithRP(keyReplicationRemoteSystem)]
		remoteUnity, err := getUnityClient(ctx, s, remoteArrayID)
		if err != nil {
			return nil, err
		}
		ctx, log = setArrayIDContext(ctx, remoteArrayID)
		if err := s.requireProbe(ctx, remoteArrayID); err != nil {
			return nil, err
		}

		replfsAPI := gounity.NewFilesystem(remoteUnity)
		fsName := strings.Split(req.GetVolumeHandle(), "-")[0] + "-" + strings.Split(req.GetVolumeHandle(), "-")[1]
		remoteFs, err := replfsAPI.FindFilesystemByName(ctx, fsName)
		log.Info("fsName ", fsName, " localArrayID: ", req.Parameters[keyArrayID], " remoteArrayID: ", remoteArrayID)
		vgPrefix := strings.ReplaceAll(fsName, remoteArrayID, req.Parameters[keyArrayID])
		remoteParams := map[string]string{
			"remoteSystem": arrayID,
			"volumeId":     remoteFs.FileContent.ID,
			"protocol":     protocol,
		}

		remoteVolume := getRemoteCSIVolume(vgPrefix+"-"+protocol+"-"+strings.ToLower(remoteArrayID)+"-"+remoteFs.FileContent.ID, int64(fileSystems.FileContent.SizeTotal))
		remoteVolume.VolumeContext = remoteParams
		return &csiext.CreateRemoteVolumeResponse{
			RemoteVolume: remoteVolume,
		}, nil
	}
	return nil, status.Error(codes.Unimplemented, "Block replication is not implemented")
}

func (s *service) CreateStorageProtectionGroup(ctx context.Context, req *csiext.CreateStorageProtectionGroupRequest) (*csiext.CreateStorageProtectionGroupResponse, error) {
	ctx, log, _ := GetRunidLog(ctx)
	log.Debugf("Executing CreateRemoteVolume with args: %s", req.String())
	volID := req.GetVolumeHandle()
	if volID == "" {
		return nil, status.Error(codes.InvalidArgument, "volume ID is required")
	}

	volID, protocol, arrayID, unity, err := s.validateAndGetResourceDetails(ctx, volID, volumeType)
	if err != nil {
		return nil, err
	}
	ctx, log = setArrayIDContext(ctx, arrayID)
	if err := s.requireProbe(ctx, arrayID); err != nil {
		return nil, err
	}

	if protocol == NFS {
		fsAPI := gounity.NewFilesystem(unity)

		fileSystems, err := fsAPI.FindFilesystemByID(ctx, volID)
		if err != nil {
			return nil, err
		}
		replAPI := gounity.NewReplicationSession(unity)
		rs, err := replAPI.FindReplicationSessionBySrcResourceID(ctx, fileSystems.FileContent.StorageResource.ID)
		if err != nil {
			return nil, err
		}
		replSession, err := replAPI.FindReplicationSessionByID(ctx, rs.ReplicationSessionContent.ReplicationSessionID)
		if err != nil {
			return nil, err
		}
		remoteProtectionGroupID := strings.Split(req.VolumeHandle, "=_=")[0]
		localParams := map[string]string{
			s.opts.replicationContextPrefix + "systemName":       arrayID,
			s.opts.replicationContextPrefix + "remoteSystemName": replSession.ReplicationSessionContent.RemoteSystem.Name,
			s.opts.replicationContextPrefix + "VolumeGroupName":  fileSystems.FileContent.Name,
			s.opts.replicationContextPrefix + "protocol":         protocol,
		}
		remoteParams := map[string]string{
			s.opts.replicationContextPrefix + "systemName":       replSession.ReplicationSessionContent.RemoteSystem.Name,
			s.opts.replicationContextPrefix + "remoteSystemName": arrayID,
			s.opts.replicationContextPrefix + "VolumeGroupName":  fileSystems.FileContent.Name,
			s.opts.replicationContextPrefix + "protocol":         protocol,
		}
		return &csiext.CreateStorageProtectionGroupResponse{
			LocalProtectionGroupId:          remoteProtectionGroupID + "::" + arrayID,
			RemoteProtectionGroupId:         remoteProtectionGroupID + "::" + replSession.ReplicationSessionContent.RemoteSystem.Name,
			LocalProtectionGroupAttributes:  localParams,
			RemoteProtectionGroupAttributes: remoteParams,
		}, nil
	}
	return nil, status.Error(codes.Unimplemented, "Block replication is not implemented")
}

func (s *service) DeleteStorageProtectionGroup(ctx context.Context, req *csiext.DeleteStorageProtectionGroupRequest) (*csiext.DeleteStorageProtectionGroupResponse, error) {
	ctx, log, _ := GetRunidLog(ctx)
	localParams := req.GetProtectionGroupAttributes()
	groupID := req.GetProtectionGroupId()
	arrayID, ok := localParams[s.opts.replicationContextPrefix+"systemName"]
	if !ok {
		log.Error("Can't get systemName from PG params")
	}

	if err := s.requireProbe(ctx, arrayID); err != nil {
		return nil, err
	}
	localUnity, err := getUnityClient(ctx, s, arrayID)
	if err != nil {
		return nil, err
	}

	fileds := map[string]interface{}{
		"ProtectedStorageGroup": groupID,
	}

	log.WithFields(fileds).Info("Deleting storage protection group")
	fsAPI := gounity.NewFilesystem(localUnity)
	prefix := strings.Split(groupID, "::")
	if len(prefix) == 0 {
		return nil, status.Error(codes.InvalidArgument, "Wrong groupID format")
	}
	fsGroup, err := fsAPI.FindFileSystemGroupByPrefixWithFields(ctx, prefix[0])
	if err != nil {
		return nil, err
	}

	if len(fsGroup) != 0 {
		return nil, status.Error(codes.Internal, "FS group is not empty")
	}

	return &csiext.DeleteStorageProtectionGroupResponse{}, nil
}

func (s *service) ExecuteAction(ctx context.Context, req *csiext.ExecuteActionRequest) (*csiext.ExecuteActionResponse, error) {
	return nil, status.Error(codes.Unimplemented, "Not implemented")
}

func (s *service) GetStorageProtectionGroupStatus(ctx context.Context, req *csiext.GetStorageProtectionGroupStatusRequest) (*csiext.GetStorageProtectionGroupStatusResponse, error) {
	localParams := req.GetProtectionGroupAttributes()
	arrayID := localParams[s.opts.replicationContextPrefix+"systemName"]
	protocol := localParams[s.opts.replicationContextPrefix+"protocol"]
	groupID := req.GetProtectionGroupId()
	prefix := strings.Split(groupID, "::")
	if len(prefix) == 0 {
		return nil, status.Error(codes.InvalidArgument, "Wrong groupID format")
	}
	vgName := prefix[0]

	ctx, log := setArrayIDContext(ctx, arrayID)
	if err := s.requireProbe(ctx, arrayID); err != nil {
		return nil, err
	}

	if protocol == NFS {
		unity, err := s.getUnityClient(ctx, arrayID)
		if err != nil {
			return nil, err
		}

		filesystemAPI := gounity.NewFilesystem(unity)
		listFS, err := filesystemAPI.FindFileSystemGroupByPrefixWithFields(ctx, vgName)
		if err != nil {
			return nil, err
		}

		if len(listFS) != 0 {
			replAPI := gounity.NewReplicationSession(unity)
			replSession, err := replAPI.FindReplicationSessionBySrcResourceID(ctx, listFS[0].FileContent.StorageResource.ID)
			if err != nil {
				return nil, err
			}
			if replSession == nil {
				log.Error("Replication session doesn't exist")
				return &csiext.GetStorageProtectionGroupStatusResponse{
					Status: &csiext.StorageProtectionGroupStatus{State: csiext.StorageProtectionGroupStatus_EMPTY},
				}, nil
			}

			rsStatus := replSession.ReplicationSessionContent.Status
			//We compare with 0 because 0 means source in unity api
			isSource := replSession.ReplicationSessionContent.LocalRole == 0

			breakParam := false
			for i, fs := range listFS {
				if i == 0 {
					continue
				}
				replSession, err = replAPI.FindReplicationSessionBySrcResourceID(ctx, fs.FileContent.StorageResource.ID)
				if err != nil {
					return nil, err
				}
				if rsStatus != replSession.ReplicationSessionContent.Status {
					breakParam = true
					break
				}
			}

			if breakParam == true {
				return &csiext.GetStorageProtectionGroupStatusResponse{
					Status: &csiext.StorageProtectionGroupStatus{State: csiext.StorageProtectionGroupStatus_UNKNOWN, IsSource: isSource},
				}, nil
			}

			var monitoringState csiext.StorageProtectionGroupStatus_State
			switch rsStatus {
			case types.RS_STATUS_ACTIVE, types.RS_STATUS_OK:
				monitoringState = csiext.StorageProtectionGroupStatus_SYNCHRONIZED
				break
			case types.RS_STATUS_SYNCING, types.RS_STATUS_MANUAL_SYNCING, types.RS_STATUS_MANUAL_SYNCING_MIXED:
				monitoringState = csiext.StorageProtectionGroupStatus_SYNC_IN_PROGRESS
				break
			case types.RS_STATUS_PAUSED, types.RS_STATUS_PAUSED_MIXED:
				monitoringState = csiext.StorageProtectionGroupStatus_SUSPENDED
				break
			case types.RS_STATUS_FAILED_OVER, types.RS_STATUS_FAILED_OVER_MIXED, types.RS_STATUS_FAILED_OVER_WITH_SYNC, types.RS_STATUS_FAILED_OVER_WITH_SYNC_MIXED:
				monitoringState = csiext.StorageProtectionGroupStatus_FAILEDOVER
				break
			case types.RS_STATUS_LOST_COMMUNICATION, types.RS_STATUS_LOST_SYNC_COMMUNICATION, types.RS_STATUS_NON_RECOVERABLE_ERROR:
				monitoringState = csiext.StorageProtectionGroupStatus_INVALID
				break
			default:
				monitoringState = csiext.StorageProtectionGroupStatus_UNKNOWN
				break
			}

			return &csiext.GetStorageProtectionGroupStatusResponse{
				Status: &csiext.StorageProtectionGroupStatus{State: monitoringState, IsSource: isSource},
			}, nil
		}
	}
	return nil, status.Error(codes.Unimplemented, "Block replication is not implemented")
}

// WithRP appends Replication Prefix to provided string
func (s *service) WithRP(key string) string {
	return s.opts.replicationPrefix + "/" + key
}

func getRemoteCSIVolume(volumeID string, size int64) *csiext.Volume {
	volume := &csiext.Volume{
		CapacityBytes: size,
		VolumeId:      volumeID,
		VolumeContext: nil,
	}
	return volume
}
