# Design the commit log

## Context

When we talk about the "log" here going forward, it refers to a type of data
structure - a sequence of records - rather than a series of events occurring in
an application for logging purposes. It can be vague when we're referring to
the log and its related components. Therefore we need to clarify the terminology
at the outset.

## Decision

We are going to build out the commit log from the bottom up. The core concepts
are:

* Log: the top-level data structure that holds a sequence of records
* Record: each individual entry of record that the log stores
* Store: the abstraction layer for storing the data on physical disks
* Index: the file that stores the index of the records
* Segment: contains the store and index files. A log often comprises of multiple
  segments

## Status

Accepted

## Consequences

Pros:

* Disambiguate the meaning of the terms, ensure consistent language

Cons:

* We will need to update this definition if anything changes in the future
