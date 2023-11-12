import asyncio
from abc import ABC, abstractmethod
from playwright.async_api import async_playwright, BrowserContext, Page, Route
from random import randint
from typing import Any, Awaitable, Callable, Iterable, Optional

from .types import ScrapeResult
from config import MAX_RETRIES, RETRY_DELAY_FACTOR, EXCLUDED_RES, WORKER_LIMIT


def _retry(max_retries=MAX_RETRIES):
    def decorator(func):
        async def wrapper(*args, **kwargs):
            attempt = 0
            while attempt < max_retries:
                attempt += 1
                try:
                    return await func(*args, **kwargs)
                except Exception as error:
                    print("Error: ", repr(error))
                    # Add incrementing delay before retrying
                    delay_duration = attempt * RETRY_DELAY_FACTOR
                    await asyncio.sleep(delay_duration)
                    print(f"Retrying in {delay_duration}s... ({attempt}/{max_retries})")

        return wrapper

    return decorator


class AsyncBaseScraper(ABC):
    @staticmethod
    @abstractmethod
    async def _create_session(
        self, urls: Iterable[str], parser: Callable[[Any], Awaitable[dict[str, Any]]]
    ) -> list[ScrapeResult]:
        pass

    @abstractmethod
    async def _worker(
        self,
        queue: asyncio.Queue,
        session: Any,
        parser: Callable[[Any], Awaitable[dict[str, Any]]],
        session_results: list[Any],
        worker_id: int,
    ) -> ScrapeResult:
        pass

    @abstractmethod
    async def _load_page(
        self, url: str, parser: Callable[[Any], Awaitable[dict[str, Any]]]
    ):
        pass

    @abstractmethod
    async def start(self):
        pass


class AsyncPlaywrightScraper(AsyncBaseScraper):
    def __init__(self, start_url: str, base_url: str):
        self.start_url = start_url
        self.base_url = base_url

    @staticmethod
    async def _handle_requests(route: Route):
        """
        Intercept request and abort requests to unneeded resources to save bandwidth
        """
        if route.request.resource_type in EXCLUDED_RES:
            await route.abort()
        else:
            await route.continue_()

    async def _create_session(
        self, urls: Iterable[str], parser: Callable[[Page], Awaitable[dict[str, Any]]]
    ) -> list[ScrapeResult]:
        session_result = []
        queue = asyncio.Queue(WORKER_LIMIT)

        # Create a browser instance
        async with async_playwright() as pw:
            print("Starting browser...")
            browser = await pw.chromium.launch(headless=False)
            try:
                context = await browser.new_context(base_url=self.base_url)
                # Handle request to save bandwidth
                await context.route("*/**", self._handle_requests)

                # Create workers
                new_tabs_count = len(urls) if len(urls) < WORKER_LIMIT else WORKER_LIMIT
                tabs = [
                    asyncio.create_task(
                        self._worker(queue, context, parser, session_result, tab_id),
                        name=tab_id,
                    )
                    for tab_id in range(1, (new_tabs_count) + 1)
                ]

                # Return a tuple containing the url index so we can keep track of them
                for index, url in enumerate(urls, 1):
                    await queue.put((index, len(urls), url))

                await queue.join()

                print(f"Stopping workers...")
                for t in tabs:
                    t.cancel()
            finally:
                print("Closing browser...")
                await browser.close()

            return session_result

    async def _worker(
        self,
        queue: asyncio.Queue,
        context: BrowserContext,
        parser: Callable[[Page], Awaitable[dict[str, Any]]],
        session_results: list[Any],
        worker_id: int,
    ):
        print(f"[Worker {worker_id}] is starting...")
        page = await context.new_page()

        while True:
            url_index, url_count, url = await queue.get()
            counter_msg = f"[URL {url_index:2d} of {url_count}]"
            result = await self._load_page(url, page, parser, worker_id, counter_msg)

            if result:
                session_results.append(
                    {"url": url, "is_successful": True, "data": result}
                )
            else:
                session_results.append(
                    {"url": url, "is_successful": False, "data": result}
                )

            sleep_time = randint(1, 3)
            print(f"[Worker {worker_id}] sleeping for {sleep_time}s...")
            await asyncio.sleep(randint(1, 3))

            queue.task_done()
            print(f"{counter_msg}[Worker {worker_id}] is done processing {url}.")

    @_retry()
    async def _load_page(
        self,
        url: str,
        page: Page,
        parser: Callable[[Page], Awaitable[dict[str, Any]]],
        worker_id: int,
        msg: Optional[str] = "",
    ):
        print(f"{msg}[Worker {worker_id}] is scraping {url}")

        print(f"{msg}[Worker {worker_id}] is opening the page...")
        await page.goto(url, wait_until="networkidle")

        print(f"{msg}[Worker {worker_id}] is parsing the page...")

        result = await asyncio.wait_for(parser(page), 3)
        if result:
            print(f"{msg}[Worker {worker_id}] result fetched successfully")
        else:
            print(f"{msg}[Worker {worker_id}] failed to fetch the result")

        return result


class AsyncEcommPlaywrightScraper(AsyncPlaywrightScraper, ABC):
    def __init__(self, start_url: str, base_url):
        super().__init__(start_url, base_url)
        self.listings = []
        self.products = []

    @abstractmethod
    async def _listing_parser(self, page: Page):
        pass

    @abstractmethod
    async def _product_parser(self, page: Page):
        pass

    async def fetch_listing(self, urls: list[str]) -> list[ScrapeResult]:
        result = await self._create_session(urls, self._listing_parser)
        if result:
            # Since the result of fetching the listing would always return a list with one element
            # we could get the urls of the product by accessing the first and only element
            self.listings = result[0]["data"]
            print(f"Found {len(self.listings) if self.listings else 0} products.")

    async def fetch_products(self, urls: list[str]) -> list[ScrapeResult]:
        result = []
        remaining_retries = 3
        while remaining_retries and urls:
            print(f"Fetching products... (Remaining attempts: {remaining_retries})")
            remaining_retries -= 1

            attempt = await self._create_session(urls, self._product_parser)
            result.extend([item for item in attempt if item["is_successful"] == True])
            urls = [item["url"] for item in attempt if item["is_successful"] == False]

            if not urls:
                break

            if remaining_retries == 0:
                result.extend(attempt)

        self.products = result

    async def start(self):
        await self.fetch_listing([self.start_url])
        if self.listings:
            await self.fetch_products(self.listings)

        return self.products
