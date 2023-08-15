env_vars_heavy_crypto_binance=${shell cat .env.heavy_crypto_binance | grep -v "\#" | xargs}
env_vars_look_on_bybit=${shell cat .env.look_on_bybit | grep -v "\#" | xargs}


heavy-crypto-binance:
	${env_vars_heavy_crypto_binance} python -m src.heavy_crypto_binance

look-on-bybit:
	${env_vars_look_on_bybit} python -m src.look_on_bybit
