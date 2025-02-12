# Universal Data API - Open Cypher Data Layer
UDA compliant data layer for OpenCypher graph databases.

## Starting the Data Layer

Map the host folder containing the config as a volume and then provide the mapped volume as the first argument.

` docker run -v ${PWD}/my_config:/root/config mimiro/opencypher-datalayer /root/config`

## Configuration

Ensure that a file with a .json extension is located in the folder mapped above. 

The following example config shows how to set up the data layer.

```json
{
  "layer_config": {
    "port": "8095",
    "service_name": "opencypher_service",
    "log_level": "DEBUG",
    "log_format": "json",
    "config_refresh_interval": "60s"
  },
  "system_config": {
    "system_type": "neo4j",
    "endpoint": "bolt://host.docker.internal:7687",
    "username": "neo4j",
    "password": "neo4j"
  },
  "dataset_definitions": [
    {
      "name": "people",
      "source_config": {
        "label" : "Person",
        "batch_size": 1000
      }
    },
    {
      "name": "companies",
      "source_config": {
        "label" : "Company",
        "batch_size": 1000
      }
    }
  ]
}
```

The layer utilises the Bolt protocol for communicating with the Open Cypher system. Ensure the endpoint is correctly configured and the appropriate user name and password provided.

The dataset definitions only require to be named and a label for those collections provided.

The batch_size property defines how many entities are written in one batch. 

If running a full sync the current implementation will delete all data associated with the dataset assigned label before populating it again. It is recommended to only run fullsync manually and operate incremental sync on a schedule.

## Limitations

As of now only write operations are supported.


