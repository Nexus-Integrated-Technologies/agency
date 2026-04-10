# Foundation Model

The repository now has an explicit canonical base layer in
`src/foundation/`.

## Foundation Owns

- groups
- chats/messages
- scheduled tasks
- router state
- execution context and boundary
- development environments
- first-class artifacts
- foundation events

## Layering Rule

Richer systems are allowed, but they must descend from this graph:

1. A runtime attaches work to a `Group`.
2. A unit of work resolves to a `Task` or message-driven turn.
3. Execution occurs through an `ExecutionContext`.
4. The runtime resolves a `DevelopmentEnvironment` when work needs a host or VM.
5. Durable outputs become `ArtifactRecord`s.
6. State changes publish `FoundationEvent`s.

If a subsystem cannot be expressed in those terms, it is either:

- a bad fit for the active architecture, or
- a candidate for `graveyard/holonic/`.

## Current Descendant

`src/nanoclaw/` is the first runtime descendant:

- `NanoclawDb` implements `FoundationStore`
- `GroupQueue` implements `WorkQueue`
- `NanoclawApp` emits foundation artifacts and events during bootstrap
- the DigitalOcean VM is modeled as a first-class `DevelopmentEnvironment`

That is the intended direction for the rest of Agency's surviving features.
