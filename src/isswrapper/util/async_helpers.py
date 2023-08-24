import asyncio
import httpx
import numpy as np


async def fetch_all(
    urls: list,
    chunk_size: int = 500,
    delay: int = 10,
    timeout: int = 30,
    max_connections: int = 10,
    max_keepalive_connections: int = 5,
):
    """
    Asynchronously performs HTTP requests for a list of URLs with the option of chunking and a delay between requests.

    :param urls: List of URLs for requests.
    :type urls: list
    :param chunk_size: Chunk size (number of URLs in a single request), defaults to 500.
    :type chunk_size: int, optional
    :param delay: Delay between chunks in seconds, defaults to 10.
    :type delay: int, optional
    :param timeout: Maximum wait time for a server response, defaults to 30 seconds.
    :type timeout: int, optional
    :param max_connections: Maximum number of concurrent connections, defaults to 10.
    :type max_connections: int, optional
    :param max_keepalive_connections: Maximum number of connections to keep alive, defaults to 5.
    :type max_keepalive_connections: int, optional
    :return: List of asynchronous response results for the requests.
    :rtype: list
    """
    chunks = [urls[i : i + chunk_size] for i in range(0, len(urls), chunk_size)]
    results = []
    async with httpx.AsyncClient(
        limits=httpx.Limits(
            max_connections=max_connections,
            max_keepalive_connections=max_keepalive_connections,
        ),
        timeout=httpx.Timeout(timeout),
    ) as client:
        for chunk in chunks:
            tasks = [asyncio.create_task(client.get(url)) for url in chunk]
            chunk_responses = await asyncio.gather(*tasks)
            results.extend(chunk_responses)

            await asyncio.sleep(delay)
    return results


def run_async(func, *args, **kwargs):
    """

    Run an asynchronous function synchronously using asyncio.run().

    This function provides a convenient way to run an asynchronous function
    synchronously using the asyncio.run() function. It takes an asynchronous
    function `func` along with its arguments and keyword arguments, and executes
    it in an event loop using asyncio.run().

    :param func: The asynchronous function to be executed.
    :type func: callable
    :param args: Arguments to be passed to the asynchronous function.
    :param kwargs: Keyword arguments to be passed to the asynchronous function.
    :return: The result returned by the asynchronous function.
    """
    return asyncio.run(func(*args, **kwargs))


# if __name__ == "__main__":
#     sem = asyncio.Semaphore(5)
#     res = run_async(
#         safe_fetch,
#         fetch_function=fetch_data,
#         semaphore=sem,
#         base_url="https://iss.moex.com/iss",
#         endpoint="/sitenews/{0}.json".format(29287),
#     )
