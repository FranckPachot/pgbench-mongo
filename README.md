# pgbench-mongo
A port of PostgreSQL PgBench to run in MongoDB Shell and test multi-document transactions in MongoDB
(Note: the data model is not optimal for a document database - it uses the PostgreSQL pgbench one)

## run pgbench-mongodb

Start MongoDB and Benchmark
```

docker compose up -d 

# helper function to run the bench

function m() {
 echo -e "\n === running pgbench-mongo $* === \n"
 docker compose exec -it mongo-1 mongosh --eval "
  load('/pgbench-mongo.js'); run({ $* }).then(() => quit())" "mongodb://mongo-1:27017/test?replicaSet=rs0&readPreference=primary"
}
function t(){ docker compose exec -it mongo-1 mongotop $* ; }
function s(){ docker compose exec -it mongo-1 mongostat $* ; }


{

# equivalent to pgbench -i -s 10

m "i: true, s:10"

# equivalent to pgbench -S -c 8 -T 120 -s 10

m "S:true, c: 8, T: 120, s: 10, 'verbose-errors':true"


# equivalent to pgbench -N -c 8 -T 120 -s 10

m "N:true, c: 8, T: 120, s: 10, 'verbose-errors':true"

# equivalent to pgbench -c 8 -T 120 -s 10

m "'max-tries': 100, c: 8, T: 120, s: 10, 'verbose-errors':false"

 } 2>&1 | tee bench.log


```

## run PostgreSQL PgBench

When comparing with PgBench on PostgreSQL, keep in mind that:
- You need an equivalent high-availability replication setup, with writes confirmed to the majority of the three nodes and WAL flushed to persistent storage.
- The testing should be conducted over a long period to account for PostgreSQL's delayed tasks, such as vacuuming and checkpointing.
- The data model is based on PostgreSQL's relational structure, which does not highlight the advantages of a document-oriented model.
- The default workload has all client updating the same value, and does not scale by design. It's not made to test scalability and behaves differently in PostgreSQL (wait-on-conflict pessimistic locking) and MongoDB (fail-on-conflict with retry logic), and cannot be compared.

```

cd ..
git clone https://github.com/zalando/patroni.git
cd patroni
docker build -t patroni .    
docker-compose up -d

# configure synchronous commit to reduce data loss (as write concern majority is used for mongodb)

docker compose exec -iT patroni1 patronictl edit-config --apply - --force <<'JSON'
{
 synchronous_mode: "on",
 synchronous_mode_strict: "on",
 "postgresql": 
   {
   "parameters":{
     "synchronous_commit": "on",
     "synchronous_standby_names": "*"
   }
  }
}
JSON

# show configuration

docker exec -it demo-patroni2 patronictl show-config
docker exec -ti demo-patroni1 patronictl list

# helper function to run the bench

function p() {
  echo -e "\n === running pgbench $* === \n"
  docker compose exec -i -e PGPASSWORD=postgres -e PGHOST=haproxy -e PGPORT=5000 patroni1 pgbench $*
}

# equivalent benchmark

{
p -i -s 10
p -S -c 8 -T 120 -s 10
p -N -c 8 -T 120 -s 10
p -c 8 -T 120 -s 10
 } 2>&1 | tee pgbench.log

```

