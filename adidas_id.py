import asyncio
import os
import json
import re
from datetime import datetime
from playwright.async_api import Page

from core.async_scraper import AsyncEcommPlaywrightScraper

os.environ["PYTHONASYNCIODEBUG"] = "1"


class AdidasScraper(AsyncEcommPlaywrightScraper):
    def __init__(self, start_url: str, base_url: str):
        super().__init__(start_url, base_url)

    async def _listing_parser(self, page: Page):
        print("    Inside listing parser")

        async def get_urls_on_page(self, page: Page):
            print("    Looking for product URLS...")
            hrefs = [
                await el.get_attribute("href")
                for el in await page.locator(
                    ".ProductCard a.gl-product-card__link"
                ).all()
            ]

            if hrefs:
                return hrefs

            return []

        async def get_next_page_url(page):
            print("    Checking for the existence of Next button...")
            next_button = (
                page.locator("a.CategoryPaginationLink").filter(has_text="Next").first
            )
            if await next_button.is_visible():
                href = await next_button.get_attribute("href")
                print(f"    Next page is {href}")
            else:
                href = ""

            return href

        product_urls = []

        while True:
            # Get the urls from the current page
            product_urls.extend(await get_urls_on_page(self, page))

            next_page_url = await get_next_page_url(page)

            if next_page_url:
                await page.goto(next_page_url, wait_until="networkidle")
            else:
                break

        return product_urls

    async def _product_parser(self, page: Page):
        async def raw_price(price: str) -> int:
            pattern = re.compile(r"[^0-9]")
            return int(re.sub(pattern, "", price))

        async def get_image_urls(page: Page) -> list[str]:
            print(f"    Getting image URLs...")
            thumbnails = await page.locator(
                "button.ProductGallery-PaginationItem"
            ).all()
            img_urls = []

            for img in thumbnails:
                img_url = await page.locator(
                    "img.TranslateOnCursorMove-ZoomedImage"
                ).get_attribute("src")
                await img.click()
                img_urls.append(img_url)

            return img_urls

        print(f"    Waiting for the product info to load")
        await page.locator("section.ProductPage-Content").wait_for()

        print(f"    Saving product info...")
        product_name = await page.locator(".ProductInformation-Name").text_content()
        product_color = await page.locator("h5.ProductDescription-Color").text_content()
        product_desc = await page.locator(
            ".ProductInformation-ShortDescription"
        ).text_content()

        if (
            await page.locator(".ProductDescription-Price .gl-price-item--sale").count()
            > 0
        ):
            product_price = await page.locator(
                ".ProductDescription-Price .gl-price-item--sale"
            ).text_content()
        else:
            product_price = await page.locator(
                ".ProductDescription-Price .gl-price-item"
            ).text_content()

        pdp_data = {
            "name": product_name,
            "color": product_color,
            "price": await raw_price(product_price),
            "description": product_desc.strip(),
            "img_urls": await get_image_urls(page),
        }

        return pdp_data


if __name__ == "__main__":
    BASE_URL = "https://www.adidas.co.id"
    START_URL = "/pria/sepatu/sepak-bola.html"

    scraper = AdidasScraper(START_URL, BASE_URL)
    result = asyncio.run(scraper.start(), debug=True)

    OUTPUT_FILE = os.path.join(
        "output",
        datetime.now().strftime("%Y%m%d_%H%M") + "_AdidasIndonesia.json",
    )

    print(OUTPUT_FILE)

    with open(OUTPUT_FILE, "w", encoding="utf8", newline="") as f:
        json.dump(result, f, indent=2)
        print("Results saved to JSON")
