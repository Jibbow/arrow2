# Arrow

[Apache Arrow][apache-arrow] is an in-memory columnar format for modern analytics that natively
supports the concepts of a null values, nested structs, and many others.

It has an IPC protocol (based on flat buffers) and a stable C-represented ABI for intra-process
communication via foreign interfaces (FFI).

The Arrow format recommends allocating memory along cache lines to leverage modern architectures, as
well as using a shared memory model that enables multi-threading.


[apache-arrow]: https://arrow.apache.org/
