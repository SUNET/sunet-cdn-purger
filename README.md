# sunet-cdnp
This is a service that monitors the output of `varnishlog` for PURGE requests
and uses MQTT to publish information about such requets to other nodes. It also
subscribes to the same type of messages from other nodes and issues equivalent
PURGE requests to the local varnish.
