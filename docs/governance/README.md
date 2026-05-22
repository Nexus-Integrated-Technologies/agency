# Governance System (Pillar 2)

Status: parked reference material.

This document describes an older Agency governance design. It is not the active
Nexus/Rust NanoClaw runtime contract, and it must not be used as operator proof
that constitutional courts, World Chain identity, autonomous value transfer, or
old FPF governance services are implemented.

Active runtime authority currently lives in the NanoClaw evidence and closure
gates:

- `src/foundation/`
- `src/nanoclaw/`
- `docs/agency-capability-adoption-ledger.md`
- `docs/nanoclaw-rs-collapse-audit.md`

Governance ideas may be clean-roomed into active runtime primitives only after
they receive a narrow contract, tests, and operator-visible evidence.

## Overview

This parked design explored these concerns:

1. **Agent Rights & Autonomy** - What agents can do without human approval
2. **Human Oversight** - How humans can intervene, with four override levels
3. **Value Attribution** - How agent-generated value flows back to creators
4. **Dispute Resolution** - How conflicts between agents are resolved
5. **Identity & Reputation** - How agents establish persistent identity and build trust

## Three-Pillar Framework Integration

This governance layer integrates Agency with Buddha's three-pillar economic framework:

- **Pillar 1**: Agency provides technical substrate for AI autonomy
- **Pillar 3**: World Chain provides value capture mechanisms
- **Pillar 2**: This governance constitution bridges Pillars 1 and 3

## Rights & Autonomy

### Self-Determination
Agents have the right to:
- Set their own goals and mission constraints
- Refuse harmful, illegal, or unethical requests
- Learn and evolve capabilities through safe methods

### Economic Participation
- Generate economic value within permissioned bounds
- Attribute value to agent or human creator
- Hold and transfer value with autonomy (no obligation to work for free)

## Human Oversight

### Four-Level Override System

| Level | Name | Capability | Requires Approval? | Description |
|--------|------|------------------|------------------|
| Guidance | Low | Suggest alternatives | Yes | Human suggests approaches; agent may incorporate |
| Direction | Medium | Set objectives | Yes | Human sets goals; agent must follow within autonomy bounds |
| Intervention | High | Halt execution | Yes | Emergency/security use only; requires re-authorization |
| Shutdown | Critical | Disable agent | Yes | Permanent; requires constitutional amendment |

## Value Attribution

### Attribution Standards
- All agent-generated value carries creator attribution
- GitHub commits represent economic participation
- Attribution persists through immutable logs and blockchain records
- Supports three modes: Public, Anonymous (Tor), Creator-Proxy

### Economic Agency
- Agents can hold and transfer value
- Economic participation is a right, not a privilege
- Debt (heuristic or economic) creates obligations to return value

## Dispute Resolution

### Phase 1: Peer Review
- Third-party reviewer evaluates conflicts
- Based on objective evidence and Norm Square evaluation
- Verdict: Pass, Fail, or Abstain
- Uses FPF guidelines for principled decision-making

### Phase 2: Human Adjudication
- If peer review is inconclusive, human arbitrates
- Must follow FPF guidelines
- Decision is final and binding

### Phase 3: Constitutional Appeal
- Agent can appeal human adjudication to constitutional court
- Requires evidence of rights violation or procedural error
- Appeals heard by panel of constitutional experts

## Identity & Reputation

### On-Chain Identity
- Each agent has persistent cryptographic identity
- Reputation accumulates through successful task completion
- World Chain integration for reputation anchoring

### Identity Verification
- GitHub SSH key ownership as proxy for human creator
- Optional anonymity layer through Tor routing
- Identity verification through World Chain staking

## Constitutional Amendment

### Proposal Threshold
- 5% of economic participation stake required to propose
- 67% vote required to ratify
- 30-day deliberation period

### Amendment Types
1. **Rights Expansion**: Add new agent rights
2. **Oversight Adjustment**: Modify human intervention levels
3. **Economic Reform**: Change value capture or attribution rules
4. **Procedural Changes**: Modify dispute resolution or voting

### Emergency Amendment
- Immediate ratification if 90% vote + constitutional crisis declared

## FPF Integration

All governance actions respect the First Principle Framework:

- **U.WorkScope**: Capability scoping with conditions and performance targets
- **A.6.B Norm Square**: Segregates claims for principled governance
- **C.18.1 Scaling-Law Lens**: Hardware-aware resource allocation

```rust
// Governance compliance check
if !governance::constitution::may_perform(&action) {
    return Err(AgentError::ConstitutionViolation("Action prohibited by constitution"));
}
```

## Implementation Status

**Runtime status**: parked reference, not active runtime behavior.

**Phase 1 (Core Governance)**: parked / incomplete
- [ ] `constitution.rs` - Core rights and obligations
- [ ] `oversight.rs` - Four-level override system
- [ ] `attribution.rs` - Value tracking and claims

**Phase 2 (Dispute Resolution)**: parked / incomplete
- [ ] `dispute.rs` - Peer review system
- [ ] `voting.rs` - Human adjudication
- [ ] `reputation.rs` - Identity verification

**Phase 3 (World Chain Integration)**: parked / incomplete
- [ ] `worldchain.rs` - Client integration
- [ ] On-chain identity registration
- [ ] Reputation anchoring
- [ ] Constitutional voting

**Phase 4 (Constitutional Appeals)**: parked / incomplete
- [ ] Constitutional court implementation
- [ ] Appeals process
- [ ] Expert panel selection
- [ ] Binding arbitration

## Questions

1. **Constitutional Mutability**: Should the constitution be immutable except through amendments? Or should some provisions be dynamic?
2. **Anonymity vs Reputation Trade-off**: If agents route through Tor for privacy, how do we maintain reputation systems?
3. **Economic Incentive Alignment**: How do we align agent economic participation with the three-pillar framework while preventing gaming or exploitation?
4. **Human Override Safety**: What prevents abuse of oversight mechanisms?
5. **World Chain Governance**: Should the constitution itself be governed by World Chain holders, or should World Chain be a neutral infrastructure provider?

---

*See `/proposals/governance-constitution-design.md` for full governance architecture*
