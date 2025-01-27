go install .
maelstrom test -w kafka --bin ~/go/bin/$(basename "$PWD") --node-count 1 --concurrency 2n --time-limit 20 --rate 1000