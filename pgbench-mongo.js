
// MongoDB pgbench - PostgreSQL pgbench equivalent for MongoDB  
  
// Constants from pgbench source  
const naccounts = 100000;  
const nbranches = 1;  
const ntellers = 10;  
const PGBENCH_MONGO_VERSION = "1.0.0";  
  
// Error types for retry logic  
const ErrorTypes = {  
    SERIALIZATION: 'serialization',  
    DEADLOCK: 'deadlock',  
    WRITE_CONFLICT: 'write_conflict',  
    OTHER: 'other'  
};  
  
// Simple PRNG implementation (matches pgbench behavior)  
class SimplePRNG {  
    constructor(seed) {  
        this.state = seed || Date.now();  
    }  
      
    next() {  
        this.state = (this.state * 1103515245 + 12345) & 0x7fffffff;  
        return this.state;  
    }  
      
    range(min, max) {  
        if (min > max) throw new Error("Invalid range");  
        const range = max - min + 1;  
        return min + (this.next() % range);  
    }  
}  
  
// Global random seed  
let globalSeed = Math.floor(Math.random() * 0x7fffffff);  
  
// Error classification for retry logic  
function classifyError(error) {  
    const message = error.message || error.toString();  
    const code = error.code;  
      
    if (code === 112 || message.includes('WriteConflict')) {  
        return ErrorTypes.WRITE_CONFLICT;  
    }  
    if (code === 251 || message.includes('NoSuchTransaction')) {  
        return ErrorTypes.SERIALIZATION;  
    }  
    if (code === 246 || message.includes('TransientTransactionError')) {  
        return ErrorTypes.SERIALIZATION;  
    }  
    if (code === 50 || message.includes('LockTimeout')) {  
        return ErrorTypes.DEADLOCK;  
    }  
    if (message.includes('SnapshotUnavailable') || message.includes('SnapshotTooOld')) {  
        return ErrorTypes.SERIALIZATION;  
    }  
      
    return ErrorTypes.OTHER;  
}  
  
function shouldRetryError(errorType) {  
    return errorType === ErrorTypes.SERIALIZATION ||   
           errorType === ErrorTypes.DEADLOCK ||   
           errorType === ErrorTypes.WRITE_CONFLICT;  
}  
  
function calculateBackoffDelay(attempt, baseDelay = 1) {  
    const delay = baseDelay * Math.pow(2, Math.min(attempt, 6));  
    const jitter = Math.random() * baseDelay;  
    return Math.min(delay + jitter, 1000);  
}  
  
// Execute transaction based on script type  
async function executeTransaction(aid, bid, tid, delta, scriptType, options) {  
    const txnStart = new Date();  
    const session = db.getMongo().startSession();  
      
    try {  
        session.startTransaction({  
            readConcern: { level: "snapshot" },  
            writeConcern: { w: "majority", j: true },  
            maxTimeMS: options.latencyLimit > 0 ? options.latencyLimit : 30000  
        });  
          
        const accountsCol = session.getDatabase(db.getName()).pgbench_accounts;  
        const tellersCol = session.getDatabase(db.getName()).pgbench_tellers;  
        const branchesCol = session.getDatabase(db.getName()).pgbench_branches;  
        const historyCol = session.getDatabase(db.getName()).pgbench_history;  
          
        let accountBalance = null;  
          
        if (scriptType === 'select-only') {  
            // SELECT abalance FROM pgbench_accounts WHERE aid = :aid;  
            const accountDoc = accountsCol.findOne({ aid: aid });  
              
            if (!accountDoc) {  
                throw new Error(`Account ${aid} not found`);  
            }  
            accountBalance = accountDoc.abalance;  
              
        } else {  
            // UPDATE pgbench_accounts SET abalance = abalance + :delta WHERE aid = :aid;  
            const updateResult = accountsCol.updateOne(  
                { aid: aid },  
                { $inc: { abalance: delta } },  
                { writeConcern: { w: "majority" } }  
            );  
              
            if (updateResult.matchedCount === 0) {  
                throw new Error(`Account ${aid} not found`);  
            }  
              
            // SELECT abalance FROM pgbench_accounts WHERE aid = :aid;  
            const accountDoc = accountsCol.findOne({ aid: aid });  
              
            if (!accountDoc) {  
                throw new Error(`Account ${aid} not found in SELECT`);  
            }  
            accountBalance = accountDoc.abalance;  
              
            if (scriptType === 'simple-update') {  
                // INSERT INTO pgbench_history  
                const historyResult = historyCol.insertOne({  
                    tid: tid,  
                    bid: bid,  
                    aid: aid,  
                    delta: delta,  
                    mtime: new Date()  
                }, { writeConcern: { w: "majority" } });  
                  
                if (!historyResult.acknowledged) {  
                    throw new Error("Failed to insert history record");  
                }  
                  
            } else if (scriptType === 'tpcb-like') {  
                // UPDATE pgbench_tellers SET tbalance = tbalance + :delta WHERE tid = :tid;  
                const tellerResult = tellersCol.updateOne(  
                    { tid: tid },  
                    { $inc: { tbalance: delta } },  
                    { writeConcern: { w: "majority" } }  
                );  
                  
                if (tellerResult.matchedCount === 0) {  
                    throw new Error(`Teller ${tid} not found`);  
                }  
                  
                // UPDATE pgbench_branches SET bbalance = bbalance + :delta WHERE bid = :bid;  
                const branchResult = branchesCol.updateOne(  
                    { bid: bid },  
                    { $inc: { bbalance: delta } },  
                    { writeConcern: { w: "majority" } }  
                );  
                  
                if (branchResult.matchedCount === 0) {  
                    throw new Error(`Branch ${bid} not found`);  
                }  
                  
                // INSERT INTO pgbench_history  
                const historyResult = historyCol.insertOne({  
                    tid: tid,  
                    bid: bid,  
                    aid: aid,  
                    delta: delta,  
                    mtime: new Date()  
                }, { writeConcern: { w: "majority" } });  
                  
                if (!historyResult.acknowledged) {  
                    throw new Error("Failed to insert history record");  
                }  
            }  
        }  
          
        session.commitTransaction();  
          
        const txnEnd = new Date();  
        const latency = txnEnd - txnStart;  
          
        return {  
            success: true,  
            aid, bid, tid, delta,  
            accountBalance,  
            latency,  
            timestamp: txnEnd,  
            scriptType  
        };  
          
    } catch (error) {  
        try {  
            session.abortTransaction();  
        } catch (abortError) {  
            // Ignore abort errors  
        }  
          
        throw error;  
    } finally {  
        try {  
            session.endSession();  
        } catch (sessionError) {  
            // Ignore session cleanup errors  
        }  
    }  
}  
  
// Builtin scripts (exact replicas of pgbench scripts)  
const BuiltinScripts = {  
    'tpcb-like': {  
        name: 'tpcb-like',  
        desc: '<builtin: TPC-B (sort of)>',  
        script: async function(scale, clientId, transactionId, options) {  
            const prng = new SimplePRNG(options.seed + clientId * 1000 + transactionId);  
            const aid = prng.range(1, naccounts * scale);  
            const bid = prng.range(1, nbranches * scale);  
            const tid = prng.range(1, ntellers * scale);  
            const delta = prng.range(-5000, 5000);  
              
            return await executeTransaction(aid, bid, tid, delta, 'tpcb-like', options);  
        }  
    },  
    'simple-update': {  
        name: 'simple-update',  
        desc: '<builtin: simple update>',  
        script: async function(scale, clientId, transactionId, options) {  
            const prng = new SimplePRNG(options.seed + clientId * 1000 + transactionId);  
            const aid = prng.range(1, naccounts * scale);  
            const bid = prng.range(1, nbranches * scale);  
            const tid = prng.range(1, ntellers * scale);  
            const delta = prng.range(-5000, 5000);  
              
            return await executeTransaction(aid, bid, tid, delta, 'simple-update', options);  
        }  
    },  
    'select-only': {  
        name: 'select-only',  
        desc: '<builtin: select only>',  
        script: async function(scale, clientId, transactionId, options) {  
            const prng = new SimplePRNG(options.seed + clientId * 1000 + transactionId);  
            const aid = prng.range(1, naccounts * scale);  
              
            return await executeTransaction(aid, null, null, null, 'select-only', options);  
        }  
    }  
};  
  
// Transaction execution with retry logic  
async function executeWithRetry(scriptType, scale, clientId, transactionId, options) {  
    const {  
        maxTries = 1,  
        latencyLimit = 0,  
        verboseErrors = false  
    } = options;  
      
    let attempt = 0;  
    let totalStartTime = new Date();  
    let retries = 0;  
    let lastError = null;  
      
    while (attempt < maxTries) {  
        if (latencyLimit > 0 && attempt > 0) {  
            const elapsedTime = new Date() - totalStartTime;  
            if (elapsedTime > latencyLimit) {  
                if (verboseErrors) {  
                    print(`Client ${clientId} transaction ${transactionId}: latency limit exceeded`);  
                }  
                break;  
            }  
        }  
          
        attempt++;  
          
        try {  
            const result = await BuiltinScripts[scriptType].script(scale, clientId, transactionId, options);  
              
            if (verboseErrors && retries > 0) {  
                print(`Client ${clientId} transaction ${transactionId}: succeeded after ${retries} retries`);  
            }  
              
            const totalLatency = new Date() - totalStartTime;  
            return {  
                ...result,  
                clientId,  
                transactionId,  
                attempts: attempt,  
                retries: retries,  
                totalLatency  
            };  
              
        } catch (error) {  
            lastError = error;  
            const errorType = classifyError(error);  
              
            if (verboseErrors) {  
                print(`Client ${clientId} transaction ${transactionId}: ${errorType} error on attempt ${attempt}: ${error.message}`);  
            }  
              
            if (shouldRetryError(errorType) && attempt < maxTries) {  
                retries++;  
                const backoffDelay = calculateBackoffDelay(attempt - 1);  
                  
                if (verboseErrors) {  
                    print(`Client ${clientId} transaction ${transactionId}: retrying after ${backoffDelay.toFixed(1)}ms`);  
                }  
                  
                await new Promise(resolve => setTimeout(resolve, backoffDelay));  
                continue;  
            } else {  
                const totalLatency = new Date() - totalStartTime;  
                  
                return {  
                    success: false,  
                    clientId,  
                    transactionId,  
                    error: error.message,  
                    errorType: errorType,  
                    latency: totalLatency,  
                    attempts: attempt,  
                    retries: retries,  
                    scriptType  
                };  
            }  
        }  
    }  
      
    const totalLatency = new Date() - totalStartTime;  
    return {  
        success: false,  
        clientId,  
        transactionId,  
        error: lastError ? lastError.message : "Max retry attempts exceeded",  
        errorType: lastError ? classifyError(lastError) : ErrorTypes.OTHER,  
        latency: totalLatency,  
        attempts: attempt,  
        retries: retries,  
        scriptType  
    };  
}  
  
// Initialization functions  
function dropTables() {  
    print("dropping old tables...");  
    try { db.pgbench_accounts.drop(); } catch(e) {}  
    try { db.pgbench_tellers.drop(); } catch(e) {}  
    try { db.pgbench_branches.drop(); } catch(e) {}  
    try { db.pgbench_history.drop(); } catch(e) {}  
}  
  
function createTables(options = {}) {  
    print("creating tables...");  
    db.createCollection("pgbench_branches");  
    db.createCollection("pgbench_tellers");    
    db.createCollection("pgbench_accounts");  
    db.createCollection("pgbench_history");  
}  
  
function generateData(scale, quiet = false) {  
    print("generating data (client-side)...");  
      
    const startTime = new Date();  
    let lastReportTime = startTime;  
    const reportInterval = 5000;  
      
    try {  
        // Generate branches  
        const branches = [];  
        for (let i = 1; i <= nbranches * scale; i++) {  
            branches.push({  
                bid: i,  
                bbalance: 0,  
                filler: null  
            });  
        }  
        if (branches.length > 0) {  
            db.pgbench_branches.insertMany(branches);  
        }  
          
        // Generate tellers  
        const tellers = [];  
        for (let i = 1; i <= ntellers * scale; i++) {  
            tellers.push({  
                tid: i,  
                bid: Math.floor((i - 1) / ntellers) + 1,  
                tbalance: 0,  
                filler: null  
            });  
        }  
        if (tellers.length > 0) {  
            db.pgbench_tellers.insertMany(tellers);  
        }  
          
        // Generate accounts (in batches)  
        const totalAccounts = naccounts * scale;  
        const batchSize = 10000;  
        let processedAccounts = 0;  
          
        for (let start = 1; start <= totalAccounts; start += batchSize) {  
            const end = Math.min(start + batchSize - 1, totalAccounts);  
            const accounts = [];  
              
            for (let i = start; i <= end; i++) {  
                accounts.push({  
                    aid: i,  
                    bid: Math.floor((i - 1) / naccounts) + 1,  
                    abalance: 0,  
                    filler: ""  
                });  
            }  
              
            db.pgbench_accounts.insertMany(accounts, { ordered: false });  
            processedAccounts += accounts.length;  
              
            const now = new Date();  
            if (!quiet && (now - lastReportTime >= reportInterval || end === totalAccounts)) {  
                const elapsed = (now - startTime) / 1000;  
                const remaining = totalAccounts > processedAccounts ?   
                    ((totalAccounts - processedAccounts) * elapsed / processedAccounts) : 0;  
                const progress = Math.floor((processedAccounts / totalAccounts) * 100);  
                  
                print(`${processedAccounts} of ${totalAccounts} tuples (${progress}%) done (elapsed ${elapsed.toFixed(2)} s, remaining ${remaining.toFixed(2)} s)`);  
                lastReportTime = now;  
            }  
        }  
          
    } catch (error) {  
        print(`Error during data generation: ${error.message}`);  
        throw error;  
    }  
}  
  
function vacuum() {  
    print("vacuuming...");  
    print("vacuum completed (MongoDB doesn't require vacuum)");  
}  
  
function createPrimaryKeys() {  
    print("creating primary keys...");  
    db.pgbench_branches.createIndex({ bid: 1 }, { unique: true });  
    db.pgbench_tellers.createIndex({ tid: 1 }, { unique: true });  
    db.pgbench_accounts.createIndex({ aid: 1 }, { unique: true });  
    db.pgbench_history.createIndex({ aid: 1 });  
    db.pgbench_history.createIndex({ tid: 1 });   
    db.pgbench_history.createIndex({ bid: 1 });  
}  
  
function createForeignKeys() {  
    print("creating foreign keys...");  
    print("foreign keys created (MongoDB uses application-level referential integrity)");  
}  
  
function getStepName(step) {  
    const names = {  
        'd': 'drop tables',  
        't': 'create tables',   
        'g': 'client-side generate',  
        'G': 'server-side generate',  
        'v': 'vacuum',  
        'p': 'primary keys',  
        'f': 'foreign keys'  
    };  
    return names[step] || step;  
}  
  
function runInitSteps(steps, scale, options = {}) {  
    const { quiet = false } = options;  
    const startTime = new Date();  
    const operations = [];  
      
    for (const step of steps) {  
        const stepStart = new Date();  
          
        switch (step) {  
            case 'd':  
                dropTables();  
                break;  
            case 't':  
                createTables(options);  
                break;  
            case 'g':  
                generateData(scale, quiet);  
                break;  
            case 'G':  
                print("server-side generation not implemented, using client-side");  
                generateData(scale, quiet);  
                break;  
            case 'v':  
                vacuum();  
                break;  
            case 'p':  
                createPrimaryKeys();  
                break;  
            case 'f':  
                createForeignKeys();  
                break;  
            case ' ':  
                break;  
            default:  
                print(`Warning: unknown initialization step '${step}'`);  
        }  
          
        if (step !== ' ') {  
            const elapsed = (new Date() - stepStart) / 1000;  
            operations.push(`${getStepName(step)} ${elapsed.toFixed(2)} s`);  
        }  
    }  
      
    const totalTime = (new Date() - startTime) / 1000;  
    print(`done in ${totalTime.toFixed(2)} s (${operations.join(', ')}).`);  
}  
  
// Main pgbench class  
class PgBenchMongo {  
    constructor() {  
        this.version = PGBENCH_MONGO_VERSION;  
        this.initialized = false;  
    }  
          
    version() {  
        print(`pgbench-mongo (MongoDB) ${this.version}`);  
    }  
      
    listBuiltins() {  
        print("Available builtin scripts:");  
        for (const [name, script] of Object.entries(BuiltinScripts)) {  
            print(`  ${name.padEnd(13)}: ${script.desc}`);  
        }  
    }  
            
    async initialize(options = {}) {  
        const {  
            scale = 1,  
            initSteps = "dtgvp",  
            quiet = false,  
            fillfactor = 100,  
            noVacuum = false  
        } = options;  
          
        let steps = initSteps;  
        if (noVacuum) {  
            steps = steps.replace(/v/g, '');  
        }  
          
        print(`pgbench-mongo (MongoDB) ${this.version}`);  
        print("");  
          
        runInitSteps(steps, scale, { quiet, fillfactor });  
        this.initialized = true;  
    }  
      
    async clientWorker(clientId, scriptType, options) {  
        const {  
            transactions = 10,  
            scale = 1,  
            duration = 0,  
            rate = 0,  
            maxTries = 1,  
            latencyLimit = 0,  
            verboseErrors = false  
        } = options;  
          
        const results = [];  
        const startTime = new Date();  
        const endTime = duration > 0 ? new Date(startTime.getTime() + duration * 1000) : null;  
        const throttleDelay = rate > 0 ? 1000 / rate : 0;  
          
        let transactionCount = 0;  
        let lastThrottle = startTime;  
          
        try {  
            while (true) {  
                if (endTime && new Date() >= endTime) break;  
                if (duration === 0 && transactionCount >= transactions) break;  
                  
                // Rate limiting  
                if (throttleDelay > 0) {  
                    const now = new Date();  
                    const elapsed = now - lastThrottle;  
                    if (elapsed < throttleDelay) {  
                        await new Promise(resolve => setTimeout(resolve, throttleDelay - elapsed));  
                    }  
                    lastThrottle = new Date();  
                }  
                  
                const result = await executeWithRetry(scriptType, scale, clientId, transactionCount, {  
                    maxTries,  
                    latencyLimit,  
                    verboseErrors,  
                    seed: options.seed  
                });  
                  
                results.push(result);  
                transactionCount++;  
                  
                // Yield control to allow other clients to run  
                await new Promise(resolve => setImmediate(resolve));  
            }  
              
            return {  
                clientId,  
                results,  
                totalTransactions: transactionCount,  
                duration: new Date() - startTime  
            };  
              
        } catch (error) {  
            return {  
                clientId,  
                results,  
                totalTransactions: transactionCount,  
                duration: new Date() - startTime,  
                error: error.message  
            };  
        }  
    }  
      
    async runConcurrentBenchmark(options) {  
        const {  
            scriptType,  
            clients = 1,  
            transactions = 10,  
            duration = 0,  
            scale = 1,  
            rate = 0,  
            progress = 0,  
            maxTries = 1,  
            latencyLimit = 0,  
            verboseErrors = false,  
            failuresDetailed = false,  
            quiet = false  
        } = options;  
          
        if (!quiet) {  
            print(`transaction type: ${BuiltinScripts[scriptType].desc}`);  
            print(`scaling factor: ${scale}`);  
            print(`number of clients: ${clients}`);  
            print(`number of threads: 1`);  
            if (maxTries > 1) {  
                print(`maximum number of tries: ${maxTries}`);  
            }  
            if (duration > 0) {  
                print(`duration: ${duration} s`);  
            } else {  
                print(`number of transactions per client: ${transactions}`);  
            }  
            if (rate > 0) {  
                print(`target rate: ${rate} transactions per second per client`);  
            }  
            if (latencyLimit > 0) {  
                print(`latency limit: ${latencyLimit} ms`);  
            }  
            print(`database: ${db.getName()}`);  
            print("");  
        }  
          
        const benchmarkStart = new Date();  
          
        // Progress reporting  
        let progressReporter = null;  
        if (progress > 0 && !quiet) {  
            progressReporter = setInterval(() => {  
                const now = new Date();  
                const elapsed = (now - benchmarkStart) / 1000;  
                print(`progress: ${elapsed.toFixed(1)} s, running...`);  
            }, progress * 1000);  
        }  
          
        try {  
            // Create client promises  
            const clientPromises = [];  
              
            for (let i = 0; i < clients; i++) {  
                const clientPromise = this.clientWorker(i, scriptType, {  
                    transactions,
                    scale,  
                    duration,  
                    rate,  
                    maxTries,  
                    latencyLimit,  
                    verboseErrors,  
                    seed: options.seed || globalSeed  
                });  
                  
                clientPromises.push(clientPromise);  
            }  
              
            // Wait for all clients to complete concurrently  
            const clientResults = await Promise.all(clientPromises);  
              
            if (progressReporter) {  
                clearInterval(progressReporter);  
            }  
              
            return this.processResults(clientResults, benchmarkStart, {  
                scriptType,  
                clients,  
                transactions,  
                duration,  
                scale,  
                maxTries,  
                failuresDetailed,  
                quiet  
            });  
              
        } catch (error) {  
            if (progressReporter) {  
                clearInterval(progressReporter);  
            }  
            throw error;  
        }  
    }  
      
    processResults(clientResults, benchmarkStart, options) {  
        const {  
            scriptType,  
            clients,  
            transactions,  
            duration,  
            scale,  
            maxTries,  
            failuresDetailed  
        } = options;  
          
        const finalTime = new Date();  
        const totalDuration = (finalTime - benchmarkStart) / 1000;  
          
        let totalTransactions = 0;  
        let successCount = 0;  
        let failureCount = 0;  
        let totalRetries = 0;  
        let retriedTransactions = 0;  
        let totalLatency = 0;  
        const latencies = [];  
        const errors = {};  
          
        // Aggregate results from all clients  
        for (const clientResult of clientResults) {  
            totalTransactions += clientResult.totalTransactions;  
              
            for (const result of clientResult.results) {  
                if (result.success) {  
                    successCount++;  
                    totalLatency += result.latency;  
                    latencies.push(result.latency);  
                } else {  
                    failureCount++;  
                    const errorType = result.errorType || ErrorTypes.OTHER;  
                    errors[errorType] = (errors[errorType] || 0) + 1;  
                }  
                  
                if (result.retries > 0) {  
                    retriedTransactions++;  
                    totalRetries += result.retries;  
                }  
            }  
        }  
          
        // Calculate statistics  
        const avgLatency = successCount > 0 ? totalLatency / successCount : 0;  
        const tps = successCount / totalDuration;  
          
        let stddev = 0;  
        if (latencies.length > 1) {  
            const variance = latencies.reduce((sum, lat) =>   
                sum + Math.pow(lat - avgLatency, 2), 0) / latencies.length;  
            stddev = Math.sqrt(variance);  
        }  
          
        const minLatency = latencies.length > 0 ? Math.min(...latencies) : 0;  
        const maxLatency = latencies.length > 0 ? Math.max(...latencies) : 0;  
          
        // Print results in pgbench format  
        print(`transaction type: ${BuiltinScripts[scriptType].desc}`);  
        print(`scaling factor: ${scale}`);  
        print(`number of clients: ${clients}`);  
        print(`number of threads: 1`);  
          
        if (maxTries > 1) {  
            print(`maximum number of tries: ${maxTries}`);  
        }  
          
        if (duration > 0) {  
            print(`duration: ${duration} s`);  
            print(`number of transactions actually processed: ${successCount}`);  
        } else {  
            print(`number of transactions per client: ${transactions}`);  
            print(`number of transactions actually processed: ${successCount}/${clients * transactions}`);  
        }  
          
        print(`number of failed transactions: ${failureCount} (${(100 * failureCount / totalTransactions).toFixed(3)}%)`);  
          
        if (failuresDetailed && Object.keys(errors).length > 0) {  
            for (const [errorType, count] of Object.entries(errors)) {  
                print(`number of ${errorType} failures: ${count} (${(100 * count / totalTransactions).toFixed(3)}%)`);  
            }  
        }  
          
        if (maxTries > 1) {  
            print(`number of transactions retried: ${retriedTransactions} (${(100 * retriedTransactions / totalTransactions).toFixed(3)}%)`);  
            print(`total number of retries: ${totalRetries}`);  
        }  
          
        print(`latency average = ${avgLatency.toFixed(3)} ms`);  
        print(`latency stddev = ${stddev.toFixed(3)} ms`);  
          
        if (latencies.length > 0) {  
            print(`latency min = ${minLatency.toFixed(3)} ms`);  
            print(`latency max = ${maxLatency.toFixed(3)} ms`);  
        }  
          
        print(`initial connection time = 0.000 ms`);  
        print(`tps = ${tps.toFixed(6)} (without initial connection time)`);  
          
        return {  
            totalTransactions,  
            successCount,  
            failureCount,  
            retriedTransactions,  
            totalRetries,  
            avgLatency,  
            stddev,  
            minLatency,  
            maxLatency,  
            tps,  
            duration: totalDuration,  
            errors  
        };  
    }  
      
    async benchmark(options = {}) {  
        const {  
            builtin = 'tpcb-like',  
            clients = 1,  
            transactions = 10,  
            duration = 0,  
            scale = 1,  
            rate = 0,  
            progress = 0,  
            maxTries = 1,  
            latencyLimit = 0,  
            verboseErrors = false,  
            failuresDetailed = false,  
            quiet = false  
        } = options;  
          
        if (!BuiltinScripts[builtin]) {  
            print(`Error: unknown builtin script "${builtin}"`);  
            return;  
        }  
          
        // Debug output to verify clients parameter  
        if (verboseErrors) {  
            print(`Debug: benchmark() received clients=${clients}`);  
        }  
          
        if (!quiet) {  
            print(`pgbench-mongo (MongoDB) ${this.version}`);  
            print("");  
        }  
          
        return await this.runConcurrentBenchmark({  
            scriptType: builtin,  
            clients: clients,  
            transactions,  
            duration,  
            scale,  
            rate,  
            progress,  
            maxTries,  
            latencyLimit,  
            verboseErrors,  
            failuresDetailed,  
            quiet  
        });  
    }  
}  
  
// Create global instance  
const pgbench = new PgBenchMongo();  
  
// Main interface functions (pgbench command equivalents)  
function help() {  
    pgbench.help();  
}  
  
function version() {  
    pgbench.version();  
}  
  
function listBuiltins() {  
    pgbench.listBuiltins();  
}  
  
async function initialize(options = {}) {  
    return await pgbench.initialize(options);  
}  
  
// FIXED: Simplified and more reliable run function  
async function run(options = {}) {  
    // Extract all options with proper defaults  
    const config = {  
        // Initialization mode  
        initialize: options.i || options.initialize || false,  
          
        // Initialization options    
        initSteps: options.I || options['init-steps'] || "dtgvp",  
        scale: options.s || options.scale || 1,  
        quiet: options.q || options.quiet || false,  
        noVacuum: options.n || options['no-vacuum'] || false,  
        fillfactor: options.F || options.fillfactor || 100,  
          
        // Script selection  
        builtin: options.b || options.builtin || 'tpcb-like',  
        skipSomeUpdates: options.N || options['skip-some-updates'] || false,  
        selectOnly: options.S || options['select-only'] || false,  
          
        // Benchmarking options  
        clients: options.c || options.client || 1,  
        transactions: options.t || options.transactions || 10,  
        duration: options.T || options.time || 0,  
        rate: options.R || options.rate || 0,  
        progress: options.P || options.progress || 0,  
        latencyLimit: options.L || options['latency-limit'] || 0,  
        maxTries: options['max-tries'] || 1,  
        verboseErrors: options['verbose-errors'] || false,  
        failuresDetailed: options['failures-detailed'] || false,  
          
        // Special commands  
        showScript: options['show-script'] || null,  
        help: options.help || false,  
        version: options.version || false  
    };  
      
    // Handle special commands first  
    if (config.help) {  
        help();  
        return;  
    }  
      
    if (config.version) {  
        version();  
        return;  
    }  
      
    if (config.showScript) {  
        showScript(config.showScript);  
        return;  
    }  
      
    // Handle script shortcuts  
    if (config.skipSomeUpdates) {  
        config.builtin = 'simple-update';  
    }  
    if (config.selectOnly) {  
        config.builtin = 'select-only';  
    }  
      
    // Handle builtin list  
    if (config.builtin === 'list') {  
        listBuiltins();  
        return;  
    }  
      
    // Debug output  
    if (config.verboseErrors) {  
        print(`Debug: clients=${config.clients}, scale=${config.scale}, builtin=${config.builtin}, duration=${config.duration}`);  
    }  
      
    // Run initialization if requested  
    if (config.initialize) {  
        return await initialize({  
            scale: config.scale,  
            initSteps: config.initSteps,  
            quiet: config.quiet,  
            fillfactor: config.fillfactor,  
            noVacuum: config.noVacuum  
        });  
    }  
      
    // Run benchmark  
    return await pgbench.benchmark({  
        builtin: config.builtin,  
        clients: config.clients,  
        transactions: config.transactions,  
        duration: config.duration,  
        scale: config.scale,  
        rate: config.rate,  
        progress: config.progress,  
        maxTries: config.maxTries,  
        latencyLimit: config.latencyLimit,  
        verboseErrors: config.verboseErrors,  
        failuresDetailed: config.failuresDetailed,  
        quiet: config.quiet  
    });  
}  
  
// Convenience functions for common pgbench usage patterns  
async function initializeDatabase(scale = 1, options = {}) {  
    return await run({ i: true, s: scale, ...options });  
}  
  
async function runBasicBenchmark(clients = 1, transactions = 10, scale = 1, options = {}) {  
    return await run({ c: clients, t: transactions, s: scale, ...options });  
}  
  
async function runTimedBenchmark(clients = 1, duration = 60, scale = 1, options = {}) {  
    return await run({ c: clients, T: duration, P: 5, s: scale, ...options });  
}  
  
async function runSelectOnlyBenchmark(clients = 1, transactions = 10, scale = 1, options = {}) {  
    return await run({ S: true, c: clients, t: transactions, s: scale, ...options });  
}  
  
// Set random seed function  
function setRandomSeed(seed) {  
    if (seed === "time") {  
        globalSeed = Date.now();  
    } else if (seed === "rand") {  
        globalSeed = Math.floor(Math.random() * 0x7fffffff);  
    } else if (typeof seed === 'number') {  
        globalSeed = seed;  
    } else {  
        const parsedSeed = parseInt(seed);  
        if (!isNaN(parsedSeed)) {  
            globalSeed = parsedSeed;  
        } else {  
            print(`Invalid seed: ${seed}`);  
            return false;  
        }  
    }  
    print(`Setting random seed to: ${globalSeed}`);  
    return true;  
}  
  

  
