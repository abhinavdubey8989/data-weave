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
        "longitude": 33.737255,
        "about": [
            "Contrary to popular belief, Lorem Ipsum is not simply random text",
            "It has roots in a piece of classical Latin literature from 45 BC",
            "making it over 2000 years old",
            "Richard McClintock, discovered the undoubtable source",
            "This book is a treatise on the theory of ethics",
            "Contrary to popular belief, Lorem Ipsum is not simply random text",
            "It has roots in a piece of classical Latin literature from 45 BC",
            "making it over 2000 years old",
            "Richard McClintock, discovered the undoubtable source",
            "This book is a treatise on the theory of ethics"
        ],
        "tools": [
            {
                "id": "tool1",
                "name": "making it over 2000 years old"
            },
            {
                "id": "tool2",
                "name": "making it over 2000 years old"
            },
            {
                "id": "tool3",
                "name": "making it over 2000 years old"
            },
            {
                "id": "tool4",
                "name": "making it over 2000 years old"
            },
            {
                "id": "tool5",
                "name": "making it over 2000 years old"
            },
            {
                "id": "tool6",
                "name": "making it over 2000 years old"
            },
            {
                "id": "tool8",
                "name": "making it over 2000 years old"
            },
            {
                "id": "tool9",
                "name": "making it over 2000 years old"
            },
            {
                "id": "tool10",
                "name": "making it over 2000 years old"
            }
        ]
    }
}'