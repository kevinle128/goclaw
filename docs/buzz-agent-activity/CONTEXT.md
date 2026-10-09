# Agent Activity

These terms describe who can see an agent's work in Buzz.

## Language

**Activity**:
The record of an agent's work, including the thinking, tool activity, and completion information available in Buzz.
_Avoid_: thinking when referring to the whole record.

**Eligible viewer**:
A user who has permission to call the agent and is a member of the channel where the work occurs.
_Avoid_: everyone without naming the channel and agent permission.

**Agent owner**:
The user who owns the agent.
Ownership is separate from permission to view its Activity.

**Channel Activity**:
The Activity associated with work in one channel, including a direct message channel.
_Avoid_: agent-wide Activity when referring to work in a specific channel.

## Transport and retained history

**Signed channel routing**:
Kind 24200 channel telemetry uses one signed `h` channel UUID, one `p` recipient, and one agent tag.
The agent signs the telemetry and encrypts a separate NIP-44 copy for each eligible viewer.
The outer channel and every decrypted batch item must agree.
See the [route parser and payload validator](/Users/dale/Desktop/workspace/opensources/buzz/crates/buzz-core/src/observer.rs:140) and [channel frame builder](/Users/dale/Desktop/workspace/opensources/buzz/crates/buzz-sdk/src/builders.rs:303).

**Current admission**:
New shared frames require current channel membership and the saved, verified owner-signed Buzz policy.
An invalid or unavailable authorization result denies shared admission.
Startup runtime response settings do not replace that saved policy.
See the [saved policy helper](/Users/dale/Desktop/workspace/opensources/buzz/crates/buzz-sdk/src/observer_policy.rs:114) and [relay authorization](/Users/dale/Desktop/workspace/opensources/buzz/crates/buzz-relay/src/observer.rs:21).

**Retained local history**:
Desktop can read valid frames already received and retained in the current identity and relay archive.
This does not add relay history, delivery to offline users, or remote removal of previously received data.
Mobile keeps its existing live-only behavior.
See the [archive admission path](/Users/dale/Desktop/workspace/opensources/buzz/desktop/src-tauri/src/archive/mod.rs:162).

The accepted [architecture decision](adr/0001-use-existing-agent-and-channel-access.md) defines the channel and legacy-control boundary.
The upstream Buzz NIP-AO Markdown update remains deferred under the user's restriction on Buzz Markdown edits.
