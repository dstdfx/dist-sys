go install .

maelstrom test \
    -w txn-rw-register \
    --bin ~/go/bin/$(basename "$PWD") \
    --node-count 2 \
    --time-limit 20 \
    --rate 1000 \
    --concurrency 2n \
    --consistency-models read-committed \
    --availability total \
    --nemesis partition
