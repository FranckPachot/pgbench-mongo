#!/bin/bash  
  
# pgbench-mongo wrapper script  
# Usage: ./pgbench-mongo.sh [mongosh-options] -- [pgbench-options]  
  
# Default values  
CLIENTS=1  
TRANSACTIONS=10  
DURATION=0  
SCALE=1  
SCRIPT="tpcb-like"  
INIT=false  
VERBOSE=false  
PROGRESS=0  
RATE=0  
MAX_TRIES=1  
  
# Parse arguments  
while [[ $# -gt 0 ]]; do  
    case $1 in  
        -c|--clients)  
            CLIENTS="$2"  
            shift 2  
            ;;  
        -t|--transactions)  
            TRANSACTIONS="$2"  
            shift 2  
            ;;  
        -T|--time)  
            DURATION="$2"  
            shift 2  
            ;;  
        -s|--scale)  
            SCALE="$2"  
            shift 2  
            ;;  
        -S|--select-only)  
            SCRIPT="select-only"  
            shift  
            ;;  
        -N|--simple-update)  
            SCRIPT="simple-update"  
            shift  
            ;;  
        -i|--initialize)  
            INIT=true  
            shift  
            ;;  
        -P|--progress)  
            PROGRESS="$2"  
            shift 2  
            ;;  
        -R|--rate)  
            RATE="$2"  
            shift 2  
            ;;  
        --max-tries)  
            MAX_TRIES="$2"  
            shift 2  
            ;;  
        --verbose-errors)  
            VERBOSE=true  
            shift  
            ;;  
        -h|--help)  
            echo "MongoDB pgbench wrapper"  
            echo "Usage: $0 [options]"  
            echo "  -c, --clients N       Number of concurrent clients"  
            echo "  -t, --transactions N  Number of transactions per client"  
            echo "  -T, --time N          Duration in seconds"  
            echo "  -s, --scale N         Scale factor"  
            echo "  -S, --select-only     SELECT-only transactions"  
            echo "  -N, --simple-update   Simple update transactions"  
            echo "  -i, --initialize      Initialize database"  
            echo "  -P, --progress N      Progress report interval"  
            echo "  -R, --rate N          Rate limit (TPS)"  
            echo "  --max-tries N         Maximum retry attempts"  
            echo "  --verbose-errors      Verbose error reporting"  
            exit 0  
            ;;  
        *)  
            #echo "Unknown option: $1"  
            #exit 1  
            args="$args $1"
            shift
            ;;  
    esac  
done  
  
# Build the command  
if [ "$INIT" = true ]; then  
    CMD="run({i: true, s: $SCALE})"  
elif [ "$SCRIPT" = "select-only" ]; then  
    CMD="run({S: true, c: $CLIENTS, t: $TRANSACTIONS, T: $DURATION, s: $SCALE, P: $PROGRESS, R: $RATE, 'max-tries': $MAX_TRIES, 'verbose-errors': $VERBOSE})"  
elif [ "$SCRIPT" = "simple-update" ]; then  
    CMD="run({N: true, c: $CLIENTS, t: $TRANSACTIONS, T: $DURATION, s: $SCALE, P: $PROGRESS, R: $RATE, 'max-tries': $MAX_TRIES, 'verbose-errors': $VERBOSE})"  
else  
    CMD="run({c: $CLIENTS, t: $TRANSACTIONS, T: $DURATION, s: $SCALE, P: $PROGRESS, R: $RATE, 'max-tries': $MAX_TRIES, 'verbose-errors': $VERBOSE})"  
fi  
  
# Execute  
set -x
mongosh --eval "load('pgbench-mongo.js'); $CMD.then(() => quit())"  $args

