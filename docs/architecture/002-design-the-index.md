# Design the index of the commit log

## Context

We are building the out the `index` structure for the store.

## Decision

The index comprises of:

* a physical file on disk
* a memory-mapped file
* the size of the index

Our index entries contain 2 fields:

* the record's offset
* its physical position in the store file

Offset refers the relative offset from the base 0, for example the first record's
offset will be 0, the second record's offset will be 1, and so on. This is
analogous to _"index"_ of an array in some programming languages.

| Offset | Position |
| ------ | -------- |
| 0      | 0        |
| 1      | 10       |
| 2      | 21       |

```
Store:    HelloWorld!Thisisthe2ndmessage.AndThisIsThe3rd ...
          ^         ^                    ^
          |         |                    |
Position: 0         10                   21
```

We are going to store offsets as `uint32` and positions as `uint64`, that means
they take up 4 and 8 bytes of space respectively.

## Status

Accepted

## Consequences

Pros:

* Storing the size of the index allows us to know where to write the next
  appended entry
* Utilizing memory-mapped file enables faster read access

Cons:

* We need to resize (`os.Truncate`) the persisted file at the beginning because
  once it's memory-mapped we can't resize it
* We also need to truncate the file (i.e. removing trailing empty spaces) before
  closing it so that the service can restarts properly
* The current design doesn't handle ungraceful shutdowns yet, we may have
  corrupted data during such situation
