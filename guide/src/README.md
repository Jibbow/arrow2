# Overview: Arrow2

[Arrow2][arrow2] is a Rust library that implements data structures and functionality enabling
interoperability with the [Apache Arrow][apache-arrow] format.

The Arrow format defines a language-independent columnar memory format for flat and hierarchical
data, organized for efficient analytic operations on modern hardware.

The typical use-case for this library is to perform CPU and memory-intensive analytics on a format
that supports heterogeneous data structures, null values, and IPC and FFI interfaces across
languages.

Arrow2 is divided into two main parts:
a [low-end API](./low_end.md) to efficiently operate with contiguous memory regions, and a
[high-end API](./high_end.md) to operate with arrow arrays, logical types, schemas, etc.

This repo started as an experiment forked from the [Apache Arrow project][apache-arrow-project] to
offer a transmute-free Rust implementation of that crate.
It currently offers most functionality with the notable exception of reading and writing to and from
parquet.


[arrow2]: https://github.com/jorgecarleitao/arrow2
[apache-arrow]: https://arrow.apache.org/
[apache-arrow-project]: https://github.com/apache/arrow
