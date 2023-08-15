env_vars_heavy_crypto_binance=${shell cat .env.heavy_crypto_binance | grep -v "\#" | xargs}


heavy-crypto-binance:
	${env_vars_heavy_crypto_binance} python -m src.heavy_crypto_binance
