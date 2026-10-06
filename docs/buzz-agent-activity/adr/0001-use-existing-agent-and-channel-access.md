---
status: accepted
---

# Use existing agent and channel access for Activity

Channel Activity is visible to users who both have permission to call the agent and belong to that channel.
This uses the existing permissions because viewing the work should not require a separate permission model.
The saved, verified owner-signed Buzz policy is the authority for Activity viewing even when runtime startup configuration differs.
It does not grant control rights or access to Activity in other channels.
Future delivery stops when either required permission is lost; existing local history remains subject to the existing product behavior.

## Channel transport contract

Reuse ephemeral observer kind 24200 and the existing Activity payload.
A new channel telemetry event has exactly one signed `h` channel UUID, one `p` recipient, and one agent tag.
The event author is the agent, and the ciphertext is a separate NIP-44 copy for that recipient.
Both agent and recipient must be actual channel members in the same community, including for open channels.
The recipient must satisfy the latest verified owner-signed saved Buzz policy and the existing owner, sibling, allowlist, nobody, and DM rules.
There is no owner membership bypass for new channel telemetry.
Malformed or unavailable authorization denies delivery rather than selecting an older permissive policy.

The receiver verifies the signature and signed recipient, decrypts the frame, and binds the envelope and each ordinary batch item to the signed channel.
Nested batches and malformed channel tags are rejected.
A malformed `h` never selects the legacy owner route.
Shared receipt does not add an agent to owner-global trust or invoke owner-management callbacks.

The relay checks current authorization during fan-out and again after socket readiness, before sink admission.
This applies to local and Redis-delivered shared frames.
The database reads are not an atomic snapshot of concurrent changes.
Bytes already admitted to the socket sink cannot be recalled.
The ACP publisher refreshes signed roster, channel metadata, profile, and saved policy before each pending recipient copy.
Its existing one-copy-per-tick limit remains in effect.

## Legacy controls and storage

Frames without signed channel routing keep the existing owner-only contract.
Owner controls and owner-management lifecycle or configuration frames keep their legacy route even when their payload mentions a channel.
Channel routing is for telemetry only; shared viewers receive no new Stop, Cancel, approval, or other control rights.
The relay does not persist observer contents.
Independent native archive admission checks current authorization and channel binding before it stores new shared ciphertext.
Replay validates the retained event for the current identity and relay and checks signature and payload binding.
Existing retained history does not require fresh viewing authorization after revocation.
No server backfill, remote history removal, new encryption key sharing, or new permission toggle is added.

## Owning source

- [Signed route parser and payload binding](/Users/dale/Desktop/workspace/opensources/buzz/crates/buzz-core/src/observer.rs:140).
- [Saved policy verification](/Users/dale/Desktop/workspace/opensources/buzz/crates/buzz-sdk/src/observer_policy.rs:114).
- [Channel event builder](/Users/dale/Desktop/workspace/opensources/buzz/crates/buzz-sdk/src/builders.rs:303).
- [Relay current authorization](/Users/dale/Desktop/workspace/opensources/buzz/crates/buzz-relay/src/observer.rs:21).
- [Socket admission boundary](/Users/dale/Desktop/workspace/opensources/buzz/crates/buzz-relay/src/connection.rs:875).
- [ACP publication](/Users/dale/Desktop/workspace/opensources/buzz/crates/buzz-acp/src/lib.rs:1208).
- [Native new-ciphertext admission](/Users/dale/Desktop/workspace/opensources/buzz/desktop/src-tauri/src/archive/mod.rs:162).
- [Desktop owner callback separation](/Users/dale/Desktop/workspace/opensources/buzz/desktop/src/features/agents/observerRelayStore.ts:476).

## Documentation boundary

The user permits Markdown changes in GoClaw and prohibits Buzz Markdown changes for this task.
This decision records the implemented contract in the permitted documentation surface.
It does not complete the planned upstream NIP-AO update.
That update must remove the obsolete owner-only description of all telemetry and document optional signed channel routing, current membership and saved-policy admission, unchanged legacy controls, batch binding, and local history.
