curl --location 'http://localhost:9111/api/v1/login' \
--header 'Content-Type: application/json' \
--data '{
    "eventCount": 1000000,
    "sleepMs": 5,
    "topic": "user_login",
    "serializationType": "AVRO",
    "batchId": "v_dkr_1",
    "jsonPayload": {
        "isActive": false,
        "picture": "http://placehold.it/32x32",
        "age": 21,
        "status": "new",
        "name": "Alice",
        "address": "829 Oxford Street, Longoria, Wyoming, 1068",
        "latitude": -85.423678,
        "longitude": 33.737255
    }
}'