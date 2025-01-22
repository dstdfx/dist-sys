go install .
maelstrom test -w broadcast --bin ~/go/bin/$(basename "$PWD") --node-count 5 --time-limit 20 --rate 10
