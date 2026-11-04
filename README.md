# sunet-cdn-purger
This is a service that monitors the output of `vinyllog` for successful PURGE
and BAN requests and uses MQTT to publish information about such requests to
other nodes. It also subscribes to the same type of messages from other nodes
and issues equivalent requests, using the same method, to the local vinyl.

Requests are replayed as they were received, before the VCL ran, so the VCL on
each node handles them the same way. For a BAN this means the VCL builds the
ban expression from the request on every node, e.g. from a header like
`Ban-Url-Regex`. A request whose method was changed to PURGE or BAN by the VCL
is not propagated.
