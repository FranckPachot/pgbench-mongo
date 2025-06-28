# pgbench-mongo
A port of PostgreSQL PgBench to run in MongoDB Shell

Start MongoDB and Benchmark
```

docker compose up -d 

function m() {
docker compose exec -it mongo-1 mongosh --eval "
 load('/pgbench-mongo.js'); run({ $* }).then(() => quit())" "mongodb://mongo-1:27017/test?replicaSet=rs0&readPreference=primary"
}

# equivalent to pgbench -i -s 10

m "i: true, s:10"

# equivalent to pgbench -S -c 32 -T 120 -s 10

m "S:true, c: 32, T: 120, s: 10, 'verbose-errors':true"


# equivalent to pgbench -N -c 32 -T 120 -s 10

m "N:true, c: 32, T: 120, s: 10, 'verbose-errors':true"

# equivalent to pgbench -c 32 -T 120 -s 10

m "'max-tries': 100, c: 32, T: 120, s: 10, 'verbose-errors':false"

```

When comparing with PgBench on PostgreSQL, keep in mind that:
- You need an equivalent high-availability replication setup, with writes confirmed to the majority of the three nodes and WAL flushed to persistent storage.
- The testing should be conducted over a long period to account for PostgreSQL's delayed tasks, such as vacuuming and checkpointing.
- The data model is based on PostgreSQL's relational structure, which does not highlight the advantages of a document-oriented model.
- The default workload has all client updating the same value, and does not scale by design. It's not made to test scalability and behaves differently in PostgreSQL (wait-on-conflict pessimistic locking) and MongoDB (fail-on-conflict with retry logic), and cannot be compared.
