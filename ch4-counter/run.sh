go install .
maelstrom test -w g-counter --bin ~/go/bin/$(basename "$PWD") --node-count 3 --rate 100 --time-limit 20 --nemesis partition