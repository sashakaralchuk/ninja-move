-- on 07.29.2023 2:16pm utc size was 279MB
-- on 07.29.2023 3:16pm utc size was 478MB
SELECT pg_size_pretty( pg_total_relation_size('monitoring_spread_v2'));


with spreads as (
	select
		symbol,
		exchange,
		cast(last_bid + (last_ask - last_bid) / 2 as float) / cast(1000000000 AS float) as market_price,
	    cast(last_ask - last_bid as float) / cast(1000000000 AS float) as spread_absolute,
	    cast(last_ask - last_bid as float) / last_ask * 100 as spread_percents,
	    timestamp
	from monitoring_spread_v2
	order by timestamp
), min_spreads as (
	select distinct on (symbol, exchange) symbol, exchange, spread_percents, timestamp
	from spreads
	order by symbol, exchange, spread_percents asc
), max_spreads as (
	select distinct on (symbol, exchange) symbol, exchange, spread_percents, timestamp
	from spreads
	order by symbol, exchange, spread_percents desc
)
select *
from max_spreads
order by spread_percents desc;

