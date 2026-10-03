package tools

// CapabilityLease pins session tools for one run.
type CapabilityLease interface {
	Tools() []Tool
	Release()
}
