import asyncio
import typing

import avantis_trader_sdk


async def main() -> typing.NoReturn:
    provider_url = "https://mainnet.base.org"
    trader_client = avantis_trader_sdk.TraderClient(provider_url)
    print("----- GETTING PAIR INFO -----")
    pairs_info = await trader_client.pairs_cache.get_pairs_info()
    print("pairs_info:", pairs_info)
    margin_fee = await trader_client.fee_parameters.get_margin_fee()
    print(
        "margin_fee:",
        margin_fee.hourly_margin_fee_long_bps["SOL/USD"],
        margin_fee.hourly_margin_fee_short_bps["SOL/USD"],
    )


if __name__ == "__main__":
    asyncio.run(main())
