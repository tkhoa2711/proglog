# Control concurrent access to the log

## Context

We need to coordinate concurrent access to the log, especially when adding new
records, to ensure data integrity.

## Decision

For the first version of the log, we are going to use a `RWMutex` at the log
level that locks for any write but allows reads when there is no write holding
the lock.

There are trade-offs in terms of performance mentioned below.

## Status

Accepted

## Consequences

Pros:

* Simple implementation at this early stage

Cons:

* Potential performance impact when the log is written more frequently
* Write to any segment will block writes to other segments as well. This could
  be optimized further by implementing locking at segment level rather than log
  level.
