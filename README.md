# BigTable emulator

Little wrapper CLI around the bigtable emulator included in the GCP Go package.

### Usage

```sh
go install ./...
```

Create a schema file and seeds file

```yml
# schema.yml
---
project: bt_test
instance: bt_test
tables:
  - name: users
    families:
      - meta
      - attributes

  - name: events
    families:
      - meta
      - attributes


```

```yml
# seeds.yml
---
users:
  "0001":
    meta:
      created_at: 2016-06-01
    attributes:
      first_name: 'David'
      last_name: 'Walsh'
  "0002":
    meta:
      created_at: 2016-07-02
    attributes:
      first_name: 'Gavin'
      last_name: 'Smith'

events:
  "0001#201606011101#page_view":
    meta:
      created_at: 2016-06-01 11:01
    attributes:
      url: "https://google.com"

```

Start the emulator

```sh
bt-emu -port 9999 -schema schema.yml -seeds seeds.yml
```


### Usage with QBT

`bt-emu` was created on a flight from Sydney to San Francisco so that I could make some changes to [`qbt`](https://github.com/catkins/qbt), my little CLI tool for interacting with BigTable via Lua scripts.

```
 qbt --emulator=localhost:9999 query users 'return row.attributes["attributes:first_name"] == "Gavin"' | jq
```
