#!/bin/bash

curl -XPOST -H "Content-type: application/json" -d @dogs.json "https://getpantry.cloud/apiv1/pantry/cd1e2a9e-7406-4c69-ad41-6ad8a75dc8be/basket/randomDogs"
