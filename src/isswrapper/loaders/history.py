from isswrapper.util.async_helpers import fetch_all
from isswrapper.util.helpers import preprocess_raw_json
import asyncio
import datetime
import nest_asyncio
import numpy as np
import pandas as pd

nest_asyncio.apply()


async def security_history(
    sec_id: str,
    engine: str,
    market: str,
    board: str,
    date_from: datetime.date = None,
    date_till: datetime.date = None,
):
    # https://iss.moex.com/iss/reference/815
    fetch_result = []
    sec_url = "https://iss.moex.com/iss/history/engines/{0}/markets/{1}/boards/{2}/securities/{3}.json".format(
        engine, market, board, sec_id
    )
    format_url = sec_url + "?start={0}"
    if date_from:
        format_url += "&from={0}".format(date_from)
    if date_till:
        format_url += "&till={0}".format(date_till)
    k = 0
    while True:
        urls = [format_url.format(i) for i in np.arange(k, k + 1000, 100)]
        tmp_res = await fetch_all(urls, delay=1)
        fetch_result.extend(tmp_res)
        if not fetch_result[-1].json()["history"]["data"]:
            break
        k += 1000
    sec_df = pd.DataFrame(
        preprocess_raw_json([resp.json() for resp in fetch_result], "history")
    )
    return sec_df


def run_async(func, *args, **kwargs):
    return asyncio.run(func(*args, **kwargs))


if __name__ == "__main__":
    import time

    st_time = time.time()
    df = asyncio.run(security_history("ABRD", "stock", "shares", "TQBR"))
    print(df.head())
    print("__________________________")
    print(df.tail())
    print(("--- %s seconds ---" % (time.time() - st_time)))
