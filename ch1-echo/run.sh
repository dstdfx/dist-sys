go install .
maelstrom test -w echo --bin ~/go/bin/$(basename "$PWD") --node-count 1 --time-limit 10
