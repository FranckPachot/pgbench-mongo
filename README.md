# pgbench-mongo
A port of PostgreSQL PgBench to run in MongoDB Shell

Start MongoDB and Benchmark
```

docker compose up -d 

function m() {
docker compose exec -it mongo mongosh --eval "
 load('/pgbench-mongo.js'); run({ $* }).then(() => quit())" "mongodb://pgbench-mongo-mongo-1:27017/test?replicaSet=rs0&readPreference=primary"
}

# equivalent to pgbench -i -s 10

m "i: true, s:10"

# equivalent to pgbench -S -c 32 -T 120 -s 10

m "S:true, c: 32, T: 120, s: 10, 'verbose-errors':true"

```

