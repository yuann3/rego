#!/bin/bash

# Connect to Redis and execute commands
redis-cli <<EOF
SET mykey "Hello Redis from script"
SET anotherkey 456
LPUSH mylist "item3"
LPUSH mylist "item4"
HSET myhash field3 "value3" field4 "value4"
SAVE
EOF

echo "RDB file created (hopefully!)"
