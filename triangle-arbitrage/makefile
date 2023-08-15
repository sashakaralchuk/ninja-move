env_vars_heavy_crypto_binance=${shell cat .env.heavy_crypto_binance | grep -v "\#" | xargs}
env_vars_look_on_bybit=${shell cat .env.look_on_bybit | grep -v "\#" | xargs}

container_name=sashakaralchuk/ninja-move-triangle-arbitrage-busybox
commit_hash=${shell git rev-parse HEAD | cut -c 1-7}


heavy-crypto-binance:
	${env_vars_heavy_crypto_binance} python -m src.heavy_crypto_binance

look-on-bybit:
	${env_vars_look_on_bybit} python -m src.look_on_bybit

build-docker-image:
	docker build \
		-t "$(container_name):$(commit_hash)" \
		--build-arg commit_hash=$(commit_hash) \
		--file dockerfile.busybox \
		--platform linux/amd64 .

push-docker-image:
	docker push "$(container_name):$(commit_hash)"
