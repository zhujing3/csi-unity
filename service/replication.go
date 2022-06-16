package service

/*
Copyright (c) 2022 Dell Technologies
All Rights Reserved
*/

// WithRP appends Replication Prefix to provided string
func (s *service) WithRP(key string) string {
	return s.replicationPrefix + "/" + key
}