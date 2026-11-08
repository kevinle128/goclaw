package acpserver

import (
	"fmt"
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
	// MaxPendingOutboundRequests bounds requests that GoClaw sends to the
	// client and that wait for a response. Client requests use their own limit.
	MaxPendingOutboundRequests = 8
	// MaxThreadContextBytes bounds the compact JSON of _meta["buzz.thread"].
	MaxThreadContextBytes   = 4 << 10
	MaxMCPServersPerSession = 8
	MaxTools                = 256
	MaxMCPEntries           = 128

	MaxGatewayFrameBytes = acpbridge.MaxGatewayFrameBytes
	// ChatSendFrameOverhead is the room in a chat.send frame for the fields
	// other than the prompt text and the thread: frame envelope, agent id,
	// session key, generation, terminal mode and the acpClient flags.
	ChatSendFrameOverhead     = 1 << 10
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

// A prompt that runs longer than its maximum duration is cancelled.
const (
	MaxPromptDurationDefault = 2 * time.Hour
	MaxPromptDurationMin     = time.Minute
	MaxPromptDurationMax     = 168 * time.Hour
)

// ValidateMaxPromptDuration rejects a duration outside the supported range.
func ValidateMaxPromptDuration(duration time.Duration) error {
	if duration < MaxPromptDurationMin || duration > MaxPromptDurationMax {
		return fmt.Errorf("max prompt duration must be between %s and %s", MaxPromptDurationMin, MaxPromptDurationMax)
	}
	return nil
}
