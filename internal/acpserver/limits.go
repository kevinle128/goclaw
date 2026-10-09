package acpserver

import (
	"time"

	"github.com/nextlevelbuilder/goclaw/internal/acpbridge"
)

const (
	MaxInputLineBytes       = 1 << 20
	MaxOutputTextChunkBytes = 64 << 10
	MaxOutputFrameBytes     = 256 << 10
	MaxPromptTextBytes      = 384 << 10
	MaxActiveSessions       = 64
	MaxOutputQueue          = 128
	MaxPendingRequests      = 256
	MaxMCPServersPerSession = 8
	MaxTools                = 256
	MaxMCPEntries           = 128

	MaxGatewayFrameBytes      = acpbridge.MaxGatewayFrameBytes
	MaxPendingGatewayRequests = 256
	MaxGatewayEventQueue      = 128
)

const (
	GatewayWriteTimeout      = 10 * time.Second
	GatewayReadTimeout       = 60 * time.Second
	GatewayControlTimeout    = 30 * time.Second
	GatewayChatTimeout       = 2 * time.Hour
	GatewayCloseDrainTimeout = 5 * time.Second
	SessionIdleTTL           = 30 * time.Minute
	SessionReapInterval      = time.Minute
)
