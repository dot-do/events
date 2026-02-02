# Architecture Decision Records

This directory contains Architecture Decision Records (ADRs) for the events.do project.

## What is an ADR?

An Architecture Decision Record is a document that captures an important architectural decision made along with its context and consequences. ADRs help:

- Document the "why" behind technical decisions
- Onboard new team members by explaining past choices
- Provide a historical record for future reference
- Enable better decision-making by learning from past choices

## ADR Index

| ADR | Title | Status | Date |
|-----|-------|--------|------|
| [0001](0001-durable-objects-for-state.md) | Durable Objects for State Management | Accepted | 2024-01-31 |
| [0002](0002-r2-for-lakehouse.md) | R2 for Event Storage (Lakehouse) | Accepted | 2024-01-31 |
| [0003](0003-queue-vs-direct-fanout.md) | Queue vs Direct Fanout Architecture | Accepted | 2024-01-31 |
| [0004](0004-sqlite-vs-kv-storage.md) | SQLite vs KV for DO Storage | Accepted | 2024-01-31 |

## ADR Template

When creating a new ADR, use this template:

```markdown
# ADR-NNNN: Title

## Status

[Proposed | Accepted | Deprecated | Superseded by ADR-XXXX]

## Context

What is the issue that we're seeing that is motivating this decision or change?

## Decision

What is the change that we're proposing and/or doing?

## Consequences

What becomes easier or more difficult to do because of this change?

### Positive

- List positive outcomes

### Negative

- List negative outcomes or trade-offs

### Risks

- List any risks and mitigation strategies
```

## Creating a New ADR

1. Copy the template above
2. Create a new file with the next sequential number: `NNNN-short-title.md`
3. Fill in all sections
4. Update the index in this README
5. Submit for review

## References

- [ADR GitHub Organization](https://adr.github.io/)
- [Documenting Architecture Decisions - Michael Nygard](https://cognitect.com/blog/2011/11/15/documenting-architecture-decisions)
