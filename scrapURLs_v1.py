import asyncio
import json
import logging
import random
from time import sleep
from fake_useragent import UserAgent
from playwright.async_api import async_playwright

# Configure logger
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

class Utils:
    """Utility functions for delays and user agent handling."""

    @staticmethod
    def get_random_user_agent():
        ua = UserAgent()
        return ua.random

    @staticmethod
    def random_delay(min_delay=1, max_delay=3):
        delay = random.uniform(min_delay, max_delay)
        logger.info("Applying delay: %.2f seconds", delay)
        sleep(delay)

    @staticmethod
    async def mimic_mouse(page):
        width, height = await page.viewport_size()
        for _ in range(5):
            x, y = random.randint(0, width), random.randint(0, height)
            logger.info("Mimicking mouse movement to: (%d, %d)", x, y)
            await page.mouse.move(x, y)
            await asyncio.sleep(random.uniform(0.5, 1.5))

class CSSSelectors:
    """Class to centralize all CSS selectors for product information extraction."""

    TITLE = "h1.product-title"
    ASIN = "span.asin-code"
    PRICE = "span.price-current"
    OLD_PRICE = "span.price-old"
    SAVINGS = "span.price-savings"
    RATING = "span.rating-value"
    RATING_COUNT = "span.rating-count"
    REVIEW_COUNT = "span.review-count"
    IMAGE = "img.product-image"
    KEY_FEATURES = "div.key-features"  # Placeholder
    CATEGORY_TREE = "ul.category-tree"  # Placeholder
    LOGIN_MODAL = "div.login-modal"  # Placeholder for login modal
    LOGIN_CLOSE_BUTTON = "button.close-modal"  # Placeholder for modal close button

class Scraper:
    """Core scraper class for handling product scraping."""

    @staticmethod
    async def handle_login_popup(page):
        """Handles login popups or modals."""
        login_modal = await page.query_selector(CSSSelectors.LOGIN_MODAL)
        if login_modal:
            logger.info("Login modal detected. Attempting to close it.")
            close_button = await login_modal.query_selector(CSSSelectors.LOGIN_CLOSE_BUTTON)
            if close_button:
                await close_button.click()
                await page.wait_for_timeout(random.uniform(1000, 2000))
                logger.info("Login modal closed successfully.")
            else:
                logger.warning("No close button found for login modal.")

    @staticmethod
    async def scrape_product_details(product_url, page):
        logger.info("Navigating to product URL: %s", product_url)
        await page.goto(product_url)
        await Scraper.handle_login_popup(page)
        await Utils.mimic_mouse(page)
        await page.wait_for_timeout(random.uniform(2000, 4000))

        # Extract specific details using selectors
        product_data = {
            "title": await page.query_selector(CSSSelectors.TITLE).text_content() if await page.query_selector(CSSSelectors.TITLE) else "",
            "ASIN": await page.query_selector(CSSSelectors.ASIN).text_content() if await page.query_selector(CSSSelectors.ASIN) else "",
            "price": await page.query_selector(CSSSelectors.PRICE).text_content() if await page.query_selector(CSSSelectors.PRICE) else "",
            "old_price": await page.query_selector(CSSSelectors.OLD_PRICE).text_content() if await page.query_selector(CSSSelectors.OLD_PRICE) else "",
            "savings": await page.query_selector(CSSSelectors.SAVINGS).text_content() if await page.query_selector(CSSSelectors.SAVINGS) else "",
            "rating": await page.query_selector(CSSSelectors.RATING).text_content() if await page.query_selector(CSSSelectors.RATING) else "",
            "rating_count": await page.query_selector(CSSSelectors.RATING_COUNT).text_content() if await page.query_selector(CSSSelectors.RATING_COUNT) else "",
            "review_count": await page.query_selector(CSSSelectors.REVIEW_COUNT).text_content() if await page.query_selector(CSSSelectors.REVIEW_COUNT) else "",
            "key_features": {},  # Needs detailed parsing based on the structure
            "category_tree": [],  # Needs detailed parsing based on the structure
            "product_url": product_url,
            "image_url": await page.query_selector(CSSSelectors.IMAGE).get_attribute("src") if await page.query_selector(CSSSelectors.IMAGE) else ""
        }

        logger.info("Scraped product details: %s", product_data)
        return product_data

    @staticmethod
    async def scrape_all_products(product_urls):
        logger.info("Starting product details scraping for %d products.", len(product_urls))
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            context = await browser.new_context(
                user_agent=Utils.get_random_user_agent()
            )
            page = await context.new_page()

            results = []
            for product_url in product_urls:
                Utils.random_delay(2, 5)
                product_data = await Scraper.scrape_product_details(product_url, page)
                results.append(product_data)

            await browser.close()

        logger.info("Completed scraping product details for all products.")
        return results

class FileHandler:
    """Class to handle file reading and writing."""

    @staticmethod
    def load_json(file_path):
        with open(file_path, "r") as f:
            return json.load(f)

    @staticmethod
    def save_json(file_path, data):
        with open(file_path, "w") as f:
            json.dump(data, f, indent=4)
        logger.info("Saved data to %s", file_path)

async def main(input_file, output_file):
    product_urls = FileHandler.load_json(input_file)
    product_details = await Scraper.scrape_all_products(product_urls)
    FileHandler.save_json(output_file, product_details)

# Example usage
if __name__ == "__main__":
    input_file = "product_urls.json"  # Replace with the actual file containing product URLs
    output_file = "product_details.json"
    asyncio.run(main(input_file, output_file))
