go install .
maelstrom test -w broadcast --bin ~/go/bin/ch-broadcast-3d --node-count 25 --time-limit 20 --rate 100 --latency 100
