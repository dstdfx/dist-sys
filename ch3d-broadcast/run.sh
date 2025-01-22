go install .
maelstrom test -w broadcast --bin ~/go/bin/$(basename "$PWD") --node-count 25 --time-limit 20 --rate 100 --latency 100
