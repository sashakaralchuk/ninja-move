env_vars=${shell cat .env | grep -v "\#" | xargs}


monitor-v1:
	${env_vars} cargo run --bin monitor -- --command v1 --write


monitor-v2:
	${env_vars} cargo run --bin monitor -- --command v2


generate-mat-views:
	${env_vars} cargo run --bin monitor -- \
		--command generate-mat-views \
		--write


trade-limit-market:
	${env_vars} cargo run --bin limit_market_v1
