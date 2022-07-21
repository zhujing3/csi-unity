package service

import (
	"context"
	"strconv"
	"strings"

	csiext "github.com/dell/dell-csi-extensions/replication"
	"github.com/dell/gounity"
	"github.com/dell/gounity/types"
	"github.com/prometheus/common/log"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func (s *service) CreateRemoteVolume(ctx context.Context, req *csiext.CreateRemoteVolumeRequest) (*csiext.CreateRemoteVolumeResponse, error) {
	ctx, log, _ := GetRunidLog(ctx)
	log.Debugf("Executing CreateRemoteVolume with args: %+v", *req)
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
		replSession, err := replAPI.FindReplicationSessionById(ctx, rs.ReplicationSessionContent.ReplicationSessionId)
		if err != nil {
			return nil, err
		}
		remoteVolumeID = replSession.ReplicationSessionContent.DstResourceId
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
	} else {
		return nil, status.Error(codes.Unimplemented, "Block replication is not implemented")
	}
}

func (s *service) CreateStorageProtectionGroup(ctx context.Context, req *csiext.CreateStorageProtectionGroupRequest) (*csiext.CreateStorageProtectionGroupResponse, error) {
	ctx, log, _ := GetRunidLog(ctx)
	log.Debugf("Executing CreateRemoteVolume with args: %+v", *req)
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
		replSession, err := replAPI.FindReplicationSessionById(ctx, rs.ReplicationSessionContent.ReplicationSessionId)
		if err != nil {
			return nil, err
		}
		remoteProtectionGroupId := strings.Split(req.VolumeHandle, "=_=")[0]
		localProtectionGroupId := strings.ReplaceAll(remoteProtectionGroupId, strings.ToUpper(req.Parameters[s.WithRP(keyReplicationRemoteSystem)]), strings.ToUpper(req.Parameters[(keyArrayID)]))
		localParams := map[string]string{
			s.opts.replicationContextPrefix + "systemName":       arrayID,
			s.opts.replicationContextPrefix + "remoteSystemName": replSession.ReplicationSessionContent.RemoteSystem.Name,
			s.opts.replicationContextPrefix + "VolumeGroupName":  fileSystems.FileContent.Name,
		}
		remoteParams := map[string]string{
			s.opts.replicationContextPrefix + "systemName":       replSession.ReplicationSessionContent.RemoteSystem.Name,
			s.opts.replicationContextPrefix + "remoteSystemName": arrayID,
			s.opts.replicationContextPrefix + "VolumeGroupName":  fileSystems.FileContent.Name,
		}
		return &csiext.CreateStorageProtectionGroupResponse{
			LocalProtectionGroupId:          localProtectionGroupId,
			RemoteProtectionGroupId:         remoteProtectionGroupId,
			LocalProtectionGroupAttributes:  localParams,
			RemoteProtectionGroupAttributes: remoteParams,
		}, nil
	} else {
		return nil, status.Error(codes.Unimplemented, "Block replication is not implemented")
	}
}
func (s *service) DeleteStorageProtectionGroup(ctx context.Context, req *csiext.DeleteStorageProtectionGroupRequest) (*csiext.DeleteStorageProtectionGroupResponse, error) {
	ctx, log, _ := GetRunidLog(ctx)
	localParams := req.GetProtectionGroupAttributes()
	groupID := req.GetProtectionGroupId()
	arrayID, ok := localParams[s.opts.replicationContextPrefix+"systemName"]
	if !ok {
		log.Error("Can't get systemName from PG params")
	}
	remoteArrayID, ok := localParams[s.opts.replicationContextPrefix+"remoteSystemName"]
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
	prefix := strings.ReplaceAll(groupID, strings.ToUpper(arrayID), strings.ToUpper(remoteArrayID))
	fsGroup, err := fsAPI.FindFileSystemGroupByPrefix(ctx, prefix)
	if err != nil {
		return nil, err
	}

	if len(fsGroup.Filesystems) != 0 {
		return nil, status.Error(codes.Internal, "FS group is not empty")
	}

	return &csiext.DeleteStorageProtectionGroupResponse{}, nil
}

func (s *service) ExecuteAction(ctx context.Context, req *csiext.ExecuteActionRequest) (*csiext.ExecuteActionResponse, error) {
	ctx, log, reqID := GetRunidLog(ctx)
	localParams := req.GetProtectionGroupAttributes()
	groupID := req.GetProtectionGroupId()
	splitedGroupID := strings.Split(strings.Split(groupID, "=_=")[0], "_")
	rpoStr := splitedGroupID[len(splitedGroupID)-1]
	rpo, err := strconv.ParseUint(rpoStr, 10, 32)
	if err != nil || uint(rpo) < uint(0) || uint(rpo) > uint(1440) {
		return nil, status.Errorf(codes.InvalidArgument, "invalid rpo value")
	}

	arrayID, ok := localParams[s.opts.replicationContextPrefix+"systemName"]
	action := req.GetAction().GetActionTypes().String()
	if !ok {
		log.Error("Can't get systemName from PG params")
	}
	remoteArrayID, ok := localParams[s.opts.replicationContextPrefix+"remoteSystemName"]
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

	if err := s.requireProbe(ctx, remoteArrayID); err != nil {
		return nil, err
	}
	remoteUnity, err := getUnityClient(ctx, s, remoteArrayID)
	if err != nil {
		return nil, err
	}

	fields := map[string]interface{}{
		"RequestID": reqID,
		"ArrayID": localParams[s.opts.replicationContextPrefix+"systemName"],
		"ProtectedStorageGroup": groupID,
		"Action": action,
	}
	log.WithFields(fields).Info("Executing ExecuteAction with following fields")
	fsAPI := gounity.NewFilesystem(localUnity)
	prefix := strings.ReplaceAll(groupID, strings.ToUpper(arrayID), strings.ToUpper(remoteArrayID))
	fsGroup, err := fsAPI.FindFileSystemGroupByPrefixWithFields(ctx, prefix)
	if err != nil {
		return nil, err
	}
	if len(fsGroup) == 0 {
		return nil, status.Error(codes.NotFound, "Filesystem group not found")
	}
	localNasServer := fsGroup[0].FileContent.NASServer

	replAPI := gounity.NewReplicationSession(localUnity)
	rs, err := replAPI.FindReplicationSessionBySrcResourceID(ctx, localNasServer.ID)
	if err != nil {
		return nil, err
	}
	var execAction gounity.ActionType
	var failOverParams *types.FailOverParams = nil
	switch action {
	case csiext.ActionTypes_FAILOVER_REMOTE.String():
		execAction = gounity.RS_ACTION_FAILOVER
		if rpo == 0 {
			failOverParams = &types.FailOverParams{Sync: false}
			resErr := FailoverAction(rs, remoteUnity, execAction, *failOverParams)
			if resErr != nil {
				return nil, resErr
			}
		} else {
			failOverParams = &types.FailOverParams{Sync: true}
			resErr := FailoverAction(rs, localUnity, execAction, *failOverParams)
			if resErr != nil {
				return nil, resErr
			}
		}
	case csiext.ActionTypes_UNPLANNED_FAILOVER_LOCAL.String():
		execAction = gounity.RS_ACTION_FAILOVER
		failOverParams = &types.FailOverParams{Sync: false}
		resErr := FailoverAction(rs, remoteUnity, execAction, *failOverParams)
		if resErr != nil {
			return nil, resErr
		}
	case csiext.ActionTypes_SUSPEND.String():
		execAction = gounity.RS_ACTION_PAUSE
		resErr := ExecuteAction(rs, localUnity, execAction)
		if resErr != nil {
			return nil, resErr
		}
	case csiext.ActionTypes_RESUME.String():
		execAction = gounity.RS_ACTION_RESUME
		resErr := ExecuteAction(rs, localUnity, execAction)
		if resErr != nil {
			return nil, resErr
		}
	case csiext.ActionTypes_SYNC.String():
		execAction = gounity.RS_ACTION_SYNC
		resErr := ExecuteAction(rs, localUnity, execAction)
		if resErr != nil {
			return nil, resErr
		}
	case csiext.ActionTypes_REPROTECT_LOCAL.String():
		execAction = gounity.RS_ACTION_REPROTECT
		resErr := ExecuteAction(rs, remoteUnity, gounity.RS_ACTION_RESUME)
		if resErr != nil {
			return nil, resErr
		}
	default:
		return nil, status.Errorf(codes.Unknown, "The requested action does not match with supported actions")
	}

	//@ TODO uncomment when GetSPGStatus will be done
	//statusResp, err := s.GetStorageProtectionGroupStatus(ctx, &csiext.GetStorageProtectionGroupStatusRequest{
	//	ProtectionGroupId: groupID,
	//	ProtectionGroupAttributes: localParams,
	//})
	//if err != nil {
	//	return nil, status.Errorf(codes.Internal, "can't get storage protection group status: %s", err.Error())
	//}
	
	resp := csiext.ExecuteActionResponse{
		Success: true,
		ActionTypes: &csiext.ExecuteActionResponse_Action{
			Action: req.GetAction(),
		},
		//@ TODO uncomment when GetSPGStatus will be done
		//Status: statusResp.Status,
	}
	return &resp, nil
}

func FailoverAction(session *types.ReplicationSession, unityClient *gounity.Client, action gounity.ActionType, failoverParams types.FailOverParams) error {
	inDesiredState, actionRequired, err := validateRSState(session, action)
	if err != nil {
		return nil
	}

	if !inDesiredState {
		if !actionRequired {
			return status.Errorf(codes.Aborted, "FailOver action: RS (%s) is still executing previous action", session.ReplicationSessionContent.ReplicationSessionId)
		}
		 replApi := gounity.NewReplicationSession(unityClient)
		 err := replApi.FailoverActionOnRelicationSessiion(context.Background(), session.ReplicationSessionContent.ReplicationSessionId, action, failoverParams)
		 if err != nil {
			return err
		 }
		 log.Debugf("Action (%s) successful on RS(%s)", string(action), session.ReplicationSessionContent.ReplicationSessionId)
	}

	return nil
}

func ExecuteAction(session *types.ReplicationSession, unityClient *gounity.Client, action gounity.ActionType) error {
	inDesiredState, actionRequired, err := validateRSState(session, action)
	if err != nil {
		return nil
	}

	if !inDesiredState {
		if !actionRequired {
			return status.Errorf(codes.Aborted, "Execute action: RS (%s) is still executing previous action", session.ReplicationSessionContent.ReplicationSessionId)
		}
		replApi := gounity.NewReplicationSession(unityClient)
		err := replApi.ExecuteActionOnReplicationSession(context.Background(), session.ReplicationSessionContent.ReplicationSessionId, action)
		if err != nil {
			return err
		}
		log.Debugf("Action (%s) successful on RS(%s)", string(action), session.ReplicationSessionContent.ReplicationSessionId)
	}

	return nil
}

func validateRSState(session *types.ReplicationSession, action gounity.ActionType) (inDesiredState, actionRequires bool, resErr error) {
	state := session.ReplicationSessionContent.Status
	log.Infof("Replication session is in %s", state)
	switch action {
	case gounity.RS_ACTION_RESUME:
		if state == 33805 || state == 2 {
			log.Infof("RS (%s) is already in desired state: (%s)", session.ReplicationSessionContent.ReplicationSessionId, state)
			return true, false, nil
		}
	case gounity.RS_ACTION_REPROTECT:
		if state == 33805 || state == 2 {
			log.Infof("RS (%s) is already in desired state: (%s)", session.ReplicationSessionContent.ReplicationSessionId, state)
			return true, false, nil
		}
	case gounity.RS_ACTION_PAUSE:
		if state == 33795 || state == 34795 {
			log.Infof("RS (%s) is already in desired state: (%s)", session.ReplicationSessionContent.ReplicationSessionId, state)
			return true, false, nil
		}
	case gounity.RS_ACTION_FAILOVER:
		if state == 33792 || state == 33793 || state == 34792 || state == 34793 {
			log.Infof("RS (%s) is already in desired state: (%s)", session.ReplicationSessionContent.ReplicationSessionId, state)
			return true, false, nil
		}
	}
	return false, true, nil	
}

func (s *service) GetStorageProtectionGroupStatus(ctx context.Context, req *csiext.GetStorageProtectionGroupStatusRequest) (*csiext.GetStorageProtectionGroupStatusResponse, error) {
	return nil, status.Error(codes.Unimplemented, "Not implemented")
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
