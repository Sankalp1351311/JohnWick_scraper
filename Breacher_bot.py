import asyncio
import sys
import time
import undetected_chromedriver as uc
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from fake_useragent import UserAgent
import nest_asyncio
import cloudscraper
import requests
from webdriver_manager.chrome import ChromeDriverManager
from webdriver_manager.core.driver import ChromeType
from selenium.webdriver.common.keys import Keys
import random
import logging
from urllib.parse import urlparse
import aiohttp

# Fix for "RuntimeError: There is no current event loop"
if sys.platform == 'win32':
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
nest_asyncio.apply()

class DynamicScraperBot:
    def __init__(self):
        self.user_agent = self.get_random_user_agent()
        self.working_proxies = []
        self.current_proxy = None
        asyncio.run(self.initialize_proxies())
        self.headers = {
            "User-Agent": self.user_agent,
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.9",
            "Accept-Encoding": "gzip, deflate, br",
            "Connection": "keep-alive",
            "Upgrade-Insecure-Requests": "1",
            "Cache-Control": "max-age=0"
        }
        self.init_logging()
        self._setup_chrome_options()

    def _setup_chrome_options(self):
        """Setup Chrome options for faster scraping."""
        try:
            # Import inside the method to handle potential import errors
            from webdriver_manager.chrome import ChromeDriverManager
            from webdriver_manager.core.driver import ChromeType
            
            # Get the latest ChromeDriver version
            latest_chromedriver_version = self.get_latest_chromedriver_version()
            
            self.chrome_options = Options()
            self.chrome_options.add_argument("--headless=new")
            self.chrome_options.add_argument("--disable-dev-shm-usage")
            self.chrome_options.add_argument("--no-sandbox")
            self.chrome_options.add_argument("--disable-gpu")
            self.chrome_options.add_argument("--disable-extensions")
            self.chrome_options.add_argument("--disable-logging")
            self.chrome_options.add_argument("--disable-images")
            self.chrome_options.add_argument("--disable-javascript")
            self.chrome_options.add_argument("--blink-settings=imagesEnabled=false")
            self.chrome_options.add_argument("--disable-notifications")
            self.chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
            self.chrome_options.add_experimental_option("useAutomationExtension", False)
            prefs = {
                "profile.managed_default_content_settings.images": 2,
                "profile.default_content_setting_values.notifications": 2,
                "profile.managed_default_content_settings.stylesheets": 2,
                "profile.managed_default_content_settings.cookies": 2,
                "profile.managed_default_content_settings.javascript": 2,
                "profile.managed_default_content_settings.plugins": 2,
                "profile.managed_default_content_settings.popups": 2,
                "profile.managed_default_content_settings.geolocation": 2,
                "profile.managed_default_content_settings.media_stream": 2,
            }
            self.chrome_options.add_experimental_option("prefs", prefs)

            # Set ChromeDriver version if successfully retrieved
            if latest_chromedriver_version:
                self.chrome_service = Service(ChromeDriverManager(version=latest_chromedriver_version, chrome_type=ChromeType.GOOGLE).install())
            else:
                # Fallback to default ChromeDriverManager
                self.chrome_service = Service(ChromeDriverManager().install())
        
        except ImportError as ie:
            logging.error(f"Import error in Chrome options setup: {ie}")
            # Fallback to standard Selenium ChromeDriver setup
            self.chrome_service = Service()
            self.chrome_options = Options()
        
        except Exception as e:
            logging.error(f"Error in Chrome options setup: {e}")
            # Fallback to standard Selenium ChromeDriver setup
            self.chrome_service = Service()
            self.chrome_options = Options()

    async def __aenter__(self):
        """Async context manager entry."""
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit."""
        await self.cleanup()

    async def cleanup(self):
        """Cleanup resources."""
        pass

    def init_logging(self):
        """Initialize logging."""
        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s - %(levelname)s - %(message)s",
            handlers=[
                logging.StreamHandler(),
                logging.FileHandler("scraper.log")
            ]
        )
        self.logger = logging.getLogger("DynamicScraperBot")

    def get_random_user_agent(self):
        """Generate a random User-Agent string."""
        try:
            ua = UserAgent()
            return ua.random
        except Exception:
            return "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"

    async def initialize_proxies(self):
        """Asynchronously initialize working proxies."""
        try:
            proxy_urls = [
                'https://raw.githubusercontent.com/TheSpeedX/PROXY-List/master/http.txt',
                'https://raw.githubusercontent.com/ShiftyTR/Proxy-List/master/http.txt',
                'https://raw.githubusercontent.com/monosans/proxy-list/main/proxies/http.txt'
            ]
            
            async with aiohttp.ClientSession() as session:
                for url in proxy_urls:
                    try:
                        async with session.get(url, timeout=10) as response:
                            if response.status == 200:
                                proxies = await response.text()
                                for proxy in proxies.split('\n'):
                                    proxy = proxy.strip()
                                    if proxy and self.is_valid_proxy(proxy):
                                        # Validate proxy more thoroughly
                                        if await self.test_proxy(session, proxy):
                                            self.working_proxies.append(proxy)
                    except Exception as e:
                        logging.warning(f"Error fetching proxies from {url}: {e}")
                
                # Select a random proxy if available
                if self.working_proxies:
                    self.current_proxy = random.choice(self.working_proxies)
                    logging.info(f"Selected proxy: {self.current_proxy}")
                    logging.info(f"Total working proxies: {len(self.working_proxies)}")
                else:
                    logging.warning("No working proxies found. Continuing without proxy.")
        except Exception as e:
            logging.error(f"Proxy initialization failed: {e}")

    async def test_proxy(self, session, proxy):
        """
        Test if a proxy is working by making a quick request.
        
        Args:
            session (aiohttp.ClientSession): Active client session
            proxy (str): Proxy URL to test
        
        Returns:
            bool: True if proxy is working, False otherwise
        """
        try:
            # Use a quick, lightweight site to test proxy
            test_url = 'http://httpbin.org/ip'
            
            # Configure proxy for the request
            proxy_url = f'http://{proxy}'
            async with session.get(test_url, proxy=proxy_url, timeout=5) as response:
                return response.status == 200
        except Exception as e:
            logging.debug(f"Proxy {proxy} failed test: {e}")
            return False

    def is_valid_proxy(self, proxy):
        """Check if a proxy is valid."""
        try:
            parsed = urlparse(proxy)
            return all([parsed.scheme, parsed.netloc])
        except Exception:
            return False

    def get_next_proxy(self):
        """Rotate to the next working proxy."""
        if not self.working_proxies:
            return None
            
        current_index = self.working_proxies.index(self.current_proxy) if self.current_proxy in self.working_proxies else -1
        next_index = (current_index + 1) % len(self.working_proxies)
        return self.working_proxies[next_index]

    async def scrape(self, url):
        """Scrape the URL using optimized Selenium."""
        try:
            return await self.scrape_with_selenium(url)
        except Exception as e:
            self.logger.error(f"Scraping failed: {e}")
            raise e

    async def scrape_with_selenium(self, url):
        """Optimized Selenium scraping."""
        max_attempts = 2
        last_error = None

        for attempt in range(max_attempts):
            try:
                options = self.chrome_options
                if self.current_proxy:
                    options.add_argument(f'--proxy-server={self.current_proxy}')
                options.add_argument(f'user-agent={self.user_agent}')
                
                driver = webdriver.Chrome(service=self.chrome_service, options=options)
                driver.set_page_load_timeout(10)  # Even shorter timeout
                
                try:
                    driver.get(url)
                    # Quick scroll to bottom and back
                    driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                    content = driver.page_source
                    return content
                finally:
                    driver.quit()
            except Exception as e:
                last_error = e
                self.logger.warning(f"Attempt {attempt + 1} failed: {e}")
                if self.working_proxies:
                    self.current_proxy = self.get_next_proxy()
                await asyncio.sleep(0.5)  # Minimal delay between retries

        raise last_error if last_error else Exception("All scraping attempts failed")

    def get_latest_chromedriver_version(self):
        """
        Get the latest ChromeDriver version compatible with the installed Google Chrome.
        
        Returns:
            str: The latest ChromeDriver version or None if retrieval fails
        """
        try:
            # Import inside the method to handle potential import errors
            from webdriver_manager.chrome import ChromeDriverManager
            from webdriver_manager.core.driver import ChromeType
            
            # Attempt to get the driver version
            version = ChromeDriverManager(chrome_type=ChromeType.GOOGLE).driver_version
            logging.info(f"Latest ChromeDriver version for Google Chrome: {version}")
            return version
        except ImportError as ie:
            logging.error(f"Import error retrieving ChromeDriver version: {ie}")
            logging.warning("Falling back to default ChromeDriver installation")
            return None
        except Exception as e:
            logging.error(f"Error retrieving ChromeDriver version: {e}")
            logging.warning("Falling back to default ChromeDriver installation")
            return None

async def main():
    print("=== Dynamic Scraping Bot ===")
    print("Installing dependencies...")
    try:
        # Pre-download ChromeDriver
        ChromeDriverManager().install()
        print("ChromeDriver installed successfully!")
        
        # Initialize the bot
        async with DynamicScraperBot() as bot:
            while True:
                url = input("\nEnter the URL to scrape (or 'quit' to exit): ")
                if url.lower() == 'quit':
                    break
                    
                try:
                    print("\nStarting scraping process...")
                    content = await bot.scrape(url)
                    print("\nScraping completed successfully!")
                    print("\nScraped Content:")
                    print("-" * 50)
                    print(content[:1000] + "..." if len(content) > 1000 else content)
                    print("-" * 50)
                    
                    # Option to save content
                    save = input("\nWould you like to save the content to a file? (y/n): ")
                    if save.lower() == 'y':
                        filename = input("Enter filename to save (e.g., output.txt): ")
                        with open(filename, 'w', encoding='utf-8') as f:
                            f.write(content)
                        print(f"Content saved to {filename}")
                except Exception as e:
                    print(f"\nAn error occurred: {e}")
                    
    except Exception as e:
        print(f"Warning: ChromeDriver installation failed: {e}")

if __name__ == "__main__":
    asyncio.run(main())
