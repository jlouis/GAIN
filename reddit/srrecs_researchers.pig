/*
psql -F"\t" -A -t -d newreddit -U ri -h $VOTEDBHOST \
     -c "\\copy (select r.rel_id, 'vote_account_link',
                        r.thing1_id, r.thing2_id, r.name, extract(epoch from r.date)
                   from reddit_rel_vote_account_link r
                   where date > now() - interval '1 week'
                ) to 'reddit_linkvote.dump'"
*/
linkvote_dump = LOAD 'reddit_linkvote.dump'
    AS (rel_id, vote_account_link_label, account_id, link_id, dir:int, timestamp);

/* psql -F"\t" -A -t -d newreddit -U ri -h $LINKDBHOST\
        -c "\\copy (select d.thing_id, 'data', 'account',
                       d.key, d.value
                  from reddit_data_account d
                 where (d.key = 'pref_public_votes' or d.key = 'pref_research')
                       and d.value='t') to 'reddit_research_ids.dump'"
*/
/* filter out only researchers */
researchers_dump = LOAD 'reddit_research_ids.dump'
                   AS (account_id, data_label, account_label, prop_name, prop_value);
researchers = FILTER researchers_dump
              BY prop_value == 't'
                 AND (prop_name == 'pref_public_votes' OR prop_name == 'pref_research');
researchers = FOREACH researchers GENERATE account_id;
researchers = DISTINCT researchers;
linkvote_votes = JOIN linkvote_dump BY account_id, researchers BY account_id PARALLEL 8;
linkvote_votes = FOREACH linkvote_votes
                 GENERATE rel_id, vote_account_link_label, linkvote_dump::account_id, link_id, dir, timestamp;

STORE linkvote_votes INTO 'researchers.out' using PigStorage('\t');
