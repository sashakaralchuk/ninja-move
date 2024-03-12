-- monitor spreads
with extended as (
	select
	    symbol,
	    cast(last_bid AS float) / cast(10000 as float) as last_bid,
	    cast(last_ask AS float) / cast(10000 AS float) as last_ask,
	    cast(last_bid + (last_ask - last_bid) / 2 as float) / cast(10000 AS float) as market_price,
	    cast(last_ask - last_bid as float) / cast(10000 AS float) as spread_absolute,
	    cast(last_ask - last_bid as float) / last_ask as spread_relative,
	    cast(last_ask - last_bid as float) / last_ask * 100 as spread_percents,
	    exchange,
	    timestamp
	from monitoring_spread
	where last_ask != 0 and last_bid != 0
), min_spread as (
	select distinct on (symbol) symbol, exchange, spread_percents, last_bid, last_ask
	from extended
	order by symbol, exchange, spread_percents asc
), max_spread as (
	select distinct on (symbol) symbol, exchange, spread_percents, last_bid, last_ask
	from extended
	order by symbol, exchange, spread_percents desc
)
select
	min_spread.symbol,
	min_spread.exchange,
	min_spread.spread_percents as spread_percents_min,
	max_spread.spread_percents as spread_percents_max,
	min_spread.last_bid as last_bid_min,
	min_spread.last_ask as last_ask_min,
	max_spread.last_bid as last_bid_max,
	max_spread.last_ask as last_ask_max
from min_spread inner join max_spread
on min_spread.symbol=max_spread.symbol
	and min_spread.exchange=max_spread.exchange;


-- notes
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
