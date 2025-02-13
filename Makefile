CH_DIRS := \
	ch1-echo \
	ch2-unique-ids \
	ch3a-broadcast \
	ch3b-broadcast \
	ch3c-broadcast \
	ch3d-broadcast \
	ch4-counter \
	ch5a-kafka \
	ch5b-kafka \
	ch5c-kafka

.PHONY: $(CH_DIRS)

$(CH_DIRS):
	cd $@ && ./run.sh

ch1: ch1-echo
ch2: ch2-unique-ids
ch3a: ch3a-broadcast
ch3b: ch3b-broadcast
ch3c: ch3c-broadcast
ch3d: ch3d-broadcast
ch4: ch4-counter
ch5a: ch5a-kafka
ch5b: ch5b-kafka
ch5c: ch5c-kafka
