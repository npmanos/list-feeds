package atpclient

import "github.com/bluesky-social/indigo/atproto/client"

var atpClient *client.APIClient

func GetATProtoClient() *client.APIClient {
	if atpClient == nil {
		atpClient = client.NewAPIClient("https://public.api.bsky.app")
	}

	return atpClient
}