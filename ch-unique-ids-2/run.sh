go install .
maelstrom test -w unique-ids --bin ~/go/bin/ch-unique-ids-2 \
                             --time-limit 30 --rate 1000 --node-count 3 \
                             --availability total --nemesis partition
