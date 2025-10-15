package atpclient

import "github.com/bluesky-social/indigo/atproto/atclient"

var atpClient *atclient.APIClient

func GetATProtoClient() *atclient.APIClient {
	if atpClient == nil {
		atpClient = atclient.NewAPIClient("https://public.api.bsky.app")
	}

	return atpClient
}
