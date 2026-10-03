package acpbridge

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/nextlevelbuilder/goclaw/internal/tools"
	"github.com/nextlevelbuilder/goclaw/pkg/protocol"
)

var (
	ErrNotFound        = errors.New("ACP capability not found")
	ErrNotOwner        = errors.New("ACP capability owner mismatch")
	ErrStaleGeneration = errors.New("ACP capability generation mismatch")
	ErrClosing         = errors.New("ACP capability is closing")
	ErrQuota           = errors.New("ACP capability quota exceeded")
	ErrTooLarge        = errors.New("ACP tool payload exceeds limit")
)

type CriticalEventSender interface {
	SendCriticalEvent(protocol.EventFrame) error
}

type Owner struct {
	ConnectionID string
	TenantID     uuid.UUID
	UserID       string
}

type ToolSchema struct {
	Name        string         `json:"name"`
	ServerName  string         `json:"serverName"`
	ToolName    string         `json:"toolName"`
	Description string         `json:"description,omitempty"`
	InputSchema map[string]any `json:"inputSchema"`
}

type Registration struct {
	Owner      Owner
	AgentID    string
	SessionKey string
	Generation uint64
	Tools      []ToolSchema
	Sender     CriticalEventSender
}

type Capability struct {
	ID        string   `json:"capabilityId"`
	ToolNames []string `json:"toolNames"`
}

type Snapshot struct {
	ID         string
	Owner      Owner
	AgentID    string
	SessionKey string
	Generation uint64
	Closing    bool
	Pins       int
}

type CallResult struct {
	Content string `json:"content"`
	IsError bool   `json:"isError"`
}

type PendingCall struct {
	ID     string
	Result <-chan CallResult
}

type Limits struct {
	CapabilityPerConnection    int
	CapabilityPerOwner         int
	CapabilityPerTenant        int
	CapabilityGlobal           int
	CallsPerCapability         int
	CallsPerConnection         int
	CallsPerOwner              int
	CallsPerTenant             int
	CallsGlobal                int
	PayloadBytes               int
	CallTimeout                time.Duration
	OrphanTTL                  time.Duration
	RegistrationsPerConnection int
	RegistrationsPerOwner      int
	RegistrationsPerTenant     int
	RegistrationWindow         time.Duration
}

func DefaultLimits() Limits {
	return Limits{
		CapabilityPerConnection: 64, CapabilityPerOwner: 128, CapabilityPerTenant: 256, CapabilityGlobal: 1024,
		CallsPerCapability: 32, CallsPerConnection: 128, CallsPerOwner: 256, CallsPerTenant: 512, CallsGlobal: 4096,
		PayloadBytes: 480 << 10, CallTimeout: 20 * time.Minute, OrphanTTL: 5 * time.Minute,
		RegistrationsPerConnection: 64, RegistrationsPerOwner: 128,
		RegistrationsPerTenant: 256, RegistrationWindow: time.Minute,
	}
}

type managerOption func(*Manager)

func WithClock(clock func() time.Time) managerOption { return func(m *Manager) { m.now = clock } }

type capabilityState struct {
	Snapshot
	tools        []ToolSchema
	sender       CriticalEventSender
	calls        map[string]*callState
	lastActivity time.Time
}

type callState struct {
	result chan CallResult
	cancel context.CancelFunc
}

type Manager struct {
	mu            sync.Mutex
	limits        Limits
	now           func() time.Time
	capabilities  map[string]*capabilityState
	registrations map[string][]time.Time
}

func NewManager(limits Limits, options ...managerOption) *Manager {
	m := &Manager{limits: limits, now: time.Now, capabilities: make(map[string]*capabilityState), registrations: make(map[string][]time.Time)}
	for _, option := range options {
		option(m)
	}
	return m
}

func (m *Manager) Register(_ context.Context, registration Registration) (*Capability, error) {
	if registration.Owner.ConnectionID == "" || registration.Owner.UserID == "" || registration.Owner.TenantID == uuid.Nil || registration.AgentID == "" || registration.SessionKey == "" || registration.Generation == 0 || registration.Sender == nil || len(registration.Tools) == 0 {
		return nil, fmt.Errorf("register ACP capability: invalid registration")
	}
	if err := m.checkSize(registration.Tools); err != nil {
		return nil, err
	}
	names := make([]string, 0, len(registration.Tools))
	seen := make(map[string]struct{}, len(registration.Tools))
	for _, schema := range registration.Tools {
		if schema.Name == "" || schema.ServerName == "" || schema.ToolName == "" {
			return nil, fmt.Errorf("register ACP capability: invalid tool schema")
		}
		if _, ok := seen[schema.Name]; ok {
			return nil, fmt.Errorf("register ACP capability: duplicate tool %q", schema.Name)
		}
		seen[schema.Name] = struct{}{}
		names = append(names, schema.Name)
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	connection, owner, tenant := m.capabilityCounts(registration.Owner)
	if len(m.capabilities) >= m.limits.CapabilityGlobal || connection >= m.limits.CapabilityPerConnection || owner >= m.limits.CapabilityPerOwner || tenant >= m.limits.CapabilityPerTenant {
		return nil, ErrQuota
	}
	if !m.allowRegistration(registration.Owner) {
		return nil, ErrQuota
	}
	id := uuid.NewString()
	m.capabilities[id] = &capabilityState{
		Snapshot: Snapshot{ID: id, Owner: registration.Owner, AgentID: registration.AgentID, SessionKey: registration.SessionKey, Generation: registration.Generation},
		tools:    append([]ToolSchema(nil), registration.Tools...), sender: registration.Sender, calls: make(map[string]*callState), lastActivity: m.now(),
	}
	return &Capability{ID: id, ToolNames: names}, nil
}

func (m *Manager) allowRegistration(owner Owner) bool {
	now := m.now()
	cutoff := now.Add(-m.limits.RegistrationWindow)
	type limitKey struct {
		key   string
		limit int
	}
	keys := []limitKey{
		{key: "connection:" + owner.ConnectionID, limit: m.limits.RegistrationsPerConnection},
		{key: "owner:" + owner.TenantID.String() + ":" + owner.UserID, limit: m.limits.RegistrationsPerOwner},
		{key: "tenant:" + owner.TenantID.String(), limit: m.limits.RegistrationsPerTenant},
	}
	for _, item := range keys {
		entries := m.registrations[item.key]
		first := 0
		for first < len(entries) && entries[first].Before(cutoff) {
			first++
		}
		entries = entries[first:]
		m.registrations[item.key] = entries
		if item.limit <= 0 || len(entries) >= item.limit {
			return false
		}
	}
	for _, item := range keys {
		m.registrations[item.key] = append(m.registrations[item.key], now)
	}
	return true
}

func (m *Manager) Snapshot(id string) (Snapshot, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	capability, ok := m.capabilities[id]
	if !ok {
		return Snapshot{}, false
	}
	return capability.Snapshot, true
}

func (m *Manager) Acquire(connectionID, sessionKey string, generation uint64) (*Lease, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, capability := range m.capabilities {
		if capability.Owner.ConnectionID == connectionID && capability.SessionKey == sessionKey && capability.Generation == generation {
			if capability.Closing {
				return nil, ErrClosing
			}
			capability.Pins++
			capability.lastActivity = m.now()
			return &Lease{manager: m, capabilityID: capability.ID}, nil
		}
	}
	return nil, ErrNotFound
}

func (m *Manager) BeginCall(ctx context.Context, capabilityID, connectionID, sessionKey string, generation uint64, toolName string, arguments map[string]any) (*PendingCall, error) {
	if err := m.checkSize(arguments); err != nil {
		return nil, err
	}
	m.mu.Lock()
	capability, ok := m.capabilities[capabilityID]
	if !ok {
		m.mu.Unlock()
		return nil, ErrNotFound
	}
	if capability.Owner.ConnectionID != connectionID || capability.SessionKey != sessionKey {
		m.mu.Unlock()
		return nil, ErrNotOwner
	}
	if capability.Generation != generation {
		m.mu.Unlock()
		return nil, ErrStaleGeneration
	}
	if capability.Closing {
		m.mu.Unlock()
		return nil, ErrClosing
	}
	if !hasTool(capability.tools, toolName) {
		m.mu.Unlock()
		return nil, ErrNotFound
	}
	connection, owner, tenant, global := m.callCounts(capability.Owner)
	if len(capability.calls) >= m.limits.CallsPerCapability || connection >= m.limits.CallsPerConnection || owner >= m.limits.CallsPerOwner || tenant >= m.limits.CallsPerTenant || global >= m.limits.CallsGlobal {
		m.mu.Unlock()
		return nil, ErrQuota
	}
	callID := uuid.NewString()
	callCtx, cancel := context.WithTimeout(ctx, m.limits.CallTimeout)
	result := make(chan CallResult, 1)
	capability.calls[callID] = &callState{result: result, cancel: cancel}
	capability.Pins++
	capability.lastActivity = m.now()
	sender := capability.sender
	payload := map[string]any{"capabilityId": capabilityID, "callId": callID, "sessionKey": sessionKey, "generation": generation, "toolName": toolName, "arguments": arguments}
	m.mu.Unlock()

	if err := sender.SendCriticalEvent(*protocol.NewEvent(protocol.EventACPToolCall, payload)); err != nil {
		m.failCall(capabilityID, callID, CallResult{Content: "ACP tool delivery failed", IsError: true})
		return nil, err
	}
	go func() {
		<-callCtx.Done()
		m.failCall(capabilityID, callID, CallResult{Content: "ACP tool call cancelled", IsError: true})
	}()
	return &PendingCall{ID: callID, Result: result}, nil
}

func (m *Manager) CompleteCall(owner Owner, capabilityID, callID string, generation uint64, result CallResult) error {
	if err := m.checkSize(result); err != nil {
		return err
	}
	m.mu.Lock()
	capability, ok := m.capabilities[capabilityID]
	if !ok {
		m.mu.Unlock()
		return ErrNotFound
	}
	if capability.Owner != owner {
		m.mu.Unlock()
		return ErrNotOwner
	}
	if capability.Generation != generation {
		m.mu.Unlock()
		return ErrStaleGeneration
	}
	call, ok := capability.calls[callID]
	if !ok {
		m.mu.Unlock()
		return ErrNotFound
	}
	delete(capability.calls, callID)
	capability.Pins--
	capability.lastActivity = m.now()
	call.cancel()
	m.deleteIfDrained(capability)
	m.mu.Unlock()
	call.result <- result
	close(call.result)
	return nil
}

func (m *Manager) Release(owner Owner, capabilityID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	capability, ok := m.capabilities[capabilityID]
	if !ok {
		return nil
	}
	if capability.Owner != owner {
		return ErrNotOwner
	}
	capability.Closing = true
	m.deleteIfDrained(capability)
	return nil
}

func (m *Manager) ReleaseConnection(connectionID string) {
	m.mu.Lock()
	var calls []*callState
	for _, capability := range m.capabilities {
		if capability.Owner.ConnectionID != connectionID {
			continue
		}
		capability.Closing = true
		for id, call := range capability.calls {
			delete(capability.calls, id)
			capability.Pins--
			calls = append(calls, call)
		}
		m.deleteIfDrained(capability)
	}
	m.mu.Unlock()
	for _, call := range calls {
		call.cancel()
		call.result <- CallResult{Content: "ACP connection closed", IsError: true}
		close(call.result)
	}
}

func (m *Manager) Close() {
	m.mu.Lock()
	connections := make(map[string]struct{})
	for _, capability := range m.capabilities {
		connections[capability.Owner.ConnectionID] = struct{}{}
	}
	m.mu.Unlock()
	for connectionID := range connections {
		m.ReleaseConnection(connectionID)
	}
}

func (m *Manager) Reap(now time.Time) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, capability := range m.capabilities {
		if capability.Pins == 0 && now.Sub(capability.lastActivity) >= m.limits.OrphanTTL {
			capability.Closing = true
			delete(m.capabilities, capability.ID)
		}
	}
	cutoff := now.Add(-m.limits.RegistrationWindow)
	for key, entries := range m.registrations {
		first := 0
		for first < len(entries) && entries[first].Before(cutoff) {
			first++
		}
		if first == len(entries) {
			delete(m.registrations, key)
		} else {
			m.registrations[key] = entries[first:]
		}
	}
}

func (m *Manager) PendingCalls() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	total := 0
	for _, capability := range m.capabilities {
		total += len(capability.calls)
	}
	return total
}

func (m *Manager) checkSize(value any) error {
	data, err := json.Marshal(value)
	if err != nil {
		return fmt.Errorf("encode ACP tool payload: %w", err)
	}
	if len(data) >= m.limits.PayloadBytes {
		return ErrTooLarge
	}
	return nil
}

func (m *Manager) failCall(capabilityID, callID string, result CallResult) {
	m.mu.Lock()
	capability, ok := m.capabilities[capabilityID]
	if !ok {
		m.mu.Unlock()
		return
	}
	call, ok := capability.calls[callID]
	if !ok {
		m.mu.Unlock()
		return
	}
	delete(capability.calls, callID)
	capability.Pins--
	call.cancel()
	m.deleteIfDrained(capability)
	m.mu.Unlock()
	call.result <- result
	close(call.result)
}

func (m *Manager) deleteIfDrained(capability *capabilityState) {
	if capability.Closing && capability.Pins == 0 {
		delete(m.capabilities, capability.ID)
	}
}

func (m *Manager) capabilityCounts(owner Owner) (connection, principal, tenant int) {
	for _, capability := range m.capabilities {
		if capability.Owner.ConnectionID == owner.ConnectionID {
			connection++
		}
		if capability.Owner.TenantID == owner.TenantID && capability.Owner.UserID == owner.UserID {
			principal++
		}
		if capability.Owner.TenantID == owner.TenantID {
			tenant++
		}
	}
	return
}

func (m *Manager) callCounts(owner Owner) (connection, principal, tenant, global int) {
	for _, capability := range m.capabilities {
		count := len(capability.calls)
		global += count
		if capability.Owner.ConnectionID == owner.ConnectionID {
			connection += count
		}
		if capability.Owner.TenantID == owner.TenantID && capability.Owner.UserID == owner.UserID {
			principal += count
		}
		if capability.Owner.TenantID == owner.TenantID {
			tenant += count
		}
	}
	return
}

func hasTool(schemas []ToolSchema, name string) bool {
	for _, schema := range schemas {
		if schema.Name == name {
			return true
		}
	}
	return false
}

type Lease struct {
	once         sync.Once
	manager      *Manager
	capabilityID string
}

func (l *Lease) Tools() []tools.Tool {
	l.manager.mu.Lock()
	defer l.manager.mu.Unlock()
	capability, ok := l.manager.capabilities[l.capabilityID]
	if !ok {
		return nil
	}
	result := make([]tools.Tool, 0, len(capability.tools))
	for _, schema := range capability.tools {
		result = append(result, &RemoteTool{manager: l.manager, capabilityID: capability.ID, owner: capability.Owner, sessionKey: capability.SessionKey, generation: capability.Generation, schema: schema})
	}
	return result
}

func (l *Lease) Release() {
	l.once.Do(func() {
		l.manager.mu.Lock()
		defer l.manager.mu.Unlock()
		capability, ok := l.manager.capabilities[l.capabilityID]
		if !ok {
			return
		}
		capability.Pins--
		capability.lastActivity = l.manager.now()
		l.manager.deleteIfDrained(capability)
	})
}
