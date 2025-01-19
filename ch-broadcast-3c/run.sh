go install .
maelstrom test -w broadcast --bin ~/go/bin/ch-broadcast-3c --node-count 5 --time-limit 20 --rate 10 --nemesis partition
