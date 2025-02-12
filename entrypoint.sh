#!/bin/sh

# Ensure the mounted /config directory is owned by the layer user
if [ -d "/config" ]; then
    chown -R layer:layer /config
fi

# Run the actual application with all passed arguments
exec /opencypher-datalayer "$@"