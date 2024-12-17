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

# Function to configure a fake user agent
def get_random_user_agent():
    ua = UserAgent()
    return ua.random

# Function to simulate human-like delays
def random_delay(min_delay=1, max_delay=3):
    delay = random.uniform(min_delay, max_delay)
    logger.info("Applying delay: %.2f seconds", delay)
    sleep(delay)

# Function to mimic mouse movements
async def mimic_mouse(page):
    width, height = await page.viewport_size()
    for _ in range(5):
        x, y = random.randint(0, width), random.randint(0, height)
        logger.info("Mimicking mouse movement to: (%d, %d)", x, y)
        await page.mouse.move(x, y)
        await asyncio.sleep(random.uniform(0.5, 1.5))

# Function to handle pagination
async def handle_pagination(page, selector, next_button_selector):
    links = []
    while True:
        logger.info("Collecting links on the current page.")
        elements = await page.query_selector_all(selector)
        for element in elements:
            url = await element.get_attribute("href")
            if url and url not in links:
                links.append(url.strip())

        next_button = await page.query_selector(next_button_selector)

        if next_button:
            logger.info("Clicking next page button.")
            await next_button.click()
            await page.wait_for_timeout(random.uniform(2000, 4000))
            await mimic_mouse(page)
        else:
            logger.info("No more pages to navigate.")
            break

    return links

# Function to handle infinite scroll
async def handle_infinite_scroll(page, selector, scroll_limit=10):
    links = []
    logger.info("Starting infinite scroll collection.")
    previous_height = 0

    for _ in range(scroll_limit):
        await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
        await page.wait_for_timeout(random.uniform(2000, 4000))  # Mimic realistic scrolling delays
        current_height = await page.evaluate("document.body.scrollHeight")
        if current_height == previous_height:
            logger.info("Reached the end of the page.")
            break
        previous_height = current_height

        elements = await page.query_selector_all(selector)
        for element in elements:
            url = await element.get_attribute("href")
            if url and url not in links:
                links.append(url.strip())

        await mimic_mouse(page)

    logger.info("Infinite scroll completed.")
    return links

# Main scraping function
async def scrape_platform(url: str, item: str, pagination=True):
    logger.info("Starting scrape for URL: %s", url)

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context(
            user_agent=get_random_user_agent()
        )
        page = await context.new_page()

        # Navigate to the URL
        logger.info("Navigating to URL: %s", url)
        await page.goto(url)
        await mimic_mouse(page)

        # Type the search item in the search box
        logger.info("Typing search item: %s", item)
        search_box_selector = "input[name='search']"  # Update with the actual selector
        await page.fill(search_box_selector, item)
        await page.press(search_box_selector, 'Enter')
        await page.wait_for_timeout(3000)

        # Collect all brand links using pagination or infinite scroll
        logger.info("Collecting all brand links.")
        brand_links_selector = "a.brand-link"  # Update with the actual selector
        next_button_selector = "button.next-page"  # Update with the actual selector

        if pagination:
            brands = await handle_pagination(page, brand_links_selector, next_button_selector)
        else:
            brands = await handle_infinite_scroll(page, brand_links_selector)

        logger.info("Found %d brands.", len(brands))

        # Iterate through each brand and collect product URLs
        for brand_url in brands:
            logger.info("Processing brand URL: %s", brand_url)
            await page.goto(brand_url)
            await page.wait_for_timeout(3000)

            product_links_selector = "a.product-link"  # Update with the actual selector

            if pagination:
                product_urls = await handle_pagination(page, product_links_selector, next_button_selector)
            else:
                product_urls = await handle_infinite_scroll(page, product_links_selector)

            # Save product URLs to JSON file
            brand_name = brand_url.split("/")[-1]
            filename = f"{brand_name}_platform.json"
            logger.info("Saving %d product URLs for brand: %s", len(product_urls), brand_name)
            with open(filename, "w") as f:
                json.dump(product_urls, f, indent=4)

        await browser.close()

    logger.info("Scraping completed.")

# Example usage
if __name__ == "__main__":
    import nest_asyncio
    from url_breacher import URLBreacher
    
    # Apply nest_asyncio to allow nested event loops
    nest_asyncio.apply()
    
    # Initialize the URL breacher
    breacher = URLBreacher()
    
    # Example URL to scrape
    url = "https://www.amazon.in/s?i=electronics&rh=n%3A1805560031&s=popularity-rank&fs=true&ref=lp_1805560031_sar"
    
    # Run the scraper
    asyncio.run(breacher.crawl(url))
