

Postgres -> Kafka:
```shell
curl -X POST -H "Content-Type: application/json" -d  '{"name":"debezium_source","config":{"tasks.max":1,"database.hostname":"postgres","database.port":5432,"database.user":"postgres","database.password":"postgres","connector.class":"io.debezium.connector.postgresql.PostgresConnector","database.dbname":"postgres","database.server.name":"dbserver1"}} ' localhost:8083/connectors
```

Kafka -> QuestDB
```shell
curl -X POST -H "Content-Type: application/json" -d '{"name":"questdb-connect","config":{"topics":"dbserver1.public.stock","table":"stock", "connector.class":"io.questdb.kafka.QuestDBSinkConnector","tasks.max":"1","key.converter":"org.apache.kafka.connect.storage.StringConverter","value.converter":"org.apache.kafka.connect.json.JsonConverter","host":"questdb", "transforms":"unwrap", "transforms.unwrap.type":"io.debezium.transforms.ExtractNewRecordState", "include.key": "false", "symbols": "symbol", "timestamp.field.name": "last_update"}}' localhost:8083/connectors
```

[Grafana Dashboard](http://localhost:3000/d/stocks/stocks?orgId=1&refresh=5s&viewPanel=2)