.PHONY: all clean run-R

SMALLCOUNT=30000
VOTES=publicvotes-20101018_votes.dump

all: affinities.erl.dump affinities.erl.small.dump

affinities.erl.small.dump:
	head -n ${SMALLCOUNT} ${VOTES} | awk 'BEGIN { OFS="\t" }; // { print $$1, $$3, $$4}' \
		| sort -S400m -k1,2 > $@

affinities.erl.dump:
	awk 'BEGIN { OFS="\t" }; // { print $$1, $$3, $$4}' ${VOTES} | sort -S400m -k1,2 > $@

affinities.dump:
	cat ${VOTES} | /srrecs.py "affinities_m()" | sort -S200m | ./srrecs.py "affinities_r()" > $@

affinities.cm affinities.clabel affinities.rlabel: affinities.dump
	cat affinities.dump | ./srrecs.py "write_matrix('affinities.cm', 'affinities.clabel', 'affinities.rlabel')"

run-R:
	time R -f ./srrecs.r

clean:
	rm -fr affinities.{dump,erl.dump,clabel,rlabel}
