.PHONY: all

VOTES=publicvotes-20101018_votes.dump

all:
	awk 'BEGIN { OFS="\t" }; // { print $$1, $$3, $$4}' ${VOTES} | sort -S400m -k1,2 > affinities.erl.dump
