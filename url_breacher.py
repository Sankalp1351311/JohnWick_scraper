import time
import asyncio
import nest_asyncio
import json
import logging
import random
from time import sleep
from fake_useragent import UserAgent
from playwright.async_api import async_playwright
from urllib.parse import urljoin, urlparse
import re
import time
from datetime import datetime
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.desired_capabilities import DesiredCapabilities
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from playwright.sync_api import sync_playwright
import cloudscraper
import requests
from webdriver_manager.chrome import ChromeDriverManager
from bs4 import BeautifulSoup
from queue import Queue
from threading import Lock
import signal
import os
from urllib.parse import parse_qsl, urlencode
import aiohttp

# Apply nest_asyncio to allow nested event loops
nest_asyncio.apply()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('url_breacher.log'),
        logging.StreamHandler()
    ]
)

class URLBreacher:
    def __init__(self, base_url=None, max_depth=3):
        self.user_agent = UserAgent().random
        self.working_proxies = []
        self.current_proxy = None
        self.base_url = base_url
        self.max_depth = max_depth
        
        # Product tracking
        self.total_products_found = 0
        self.products_per_page = []
        self.last_save_count = 0
        self.save_batch_size = 10
        
        # File handling
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        self.output_file = f'scraped_urls_{timestamp}.json'
        
        self.visited_urls = set()
        self.url_queue = Queue()
        self.url_patterns = {}
        self.url_lock = Lock()
        
        # Debug flags
        self.debug = True
        
        self.initialize_proxies()
        
        # URL Storage
        self.product_urls = set()
        self.category_urls = set()
        self.pagination_urls = set()
        
        # Site-specific selectors
        self.site_selectors = {
            'alibaba': {
                'product': ['div.product-card', 'div.product-item'],
                'link': ['a.product-link', 'a[href*="/product/"]'],
                'next_page': ['a.next-page', 'button.next-btn'],
                'infinite_scroll': True
            },
            'noon': {
                'product': ['div.product-grid-item', 'div.productContainer'],
                'link': ['a[href*="/product"]', 'a.product-link'],
                'next_page': ['button[class*="next"]', 'a.next-page'],
                'infinite_scroll': True
            },
            'sharafdg': {
                'product': ['div.product-item', 'div.product-box'],
                'link': ['a.product-url', 'a[href*="/p/"]'],
                'next_page': ['a.next', 'button.load-more'],
                'infinite_scroll': False
            },
            'amazon': {
                'product': [
                    'div[data-component-type="s-search-result"]',
                    'div.s-result-item:not(.AdHolder)',
                    '.s-card-container'
                ],
                'link': [
                    'h2 a.a-link-normal',
                    'a.a-link-normal.s-no-outline',
                    'h2.a-size-mini a',
                    'a[href*="/dp/"]'
                ],
                'next_page': [
                    '.s-pagination-next',
                    'a[href*="page="]',
                    'span.s-pagination-next'
                ],
                'infinite_scroll': False
            },
            'generic': {
                'product': ['div.product', 'div[class*="product"]', 'article.product'],
                'link': ['a[href*="product"]', 'a[href*="/p/"]', 'a.product-link'],
                'next_page': ['a.next', 'a[rel="next"]', 'button.load-more'],
                'infinite_scroll': False
            }
        }
        
        # Statistics
        self.stats = {
            'total_urls_found': 0,
            'successful_scrapes': 0,
            'failed_scrapes': 0,
            'start_time': None,
            'end_time': None,
            'products_per_page': [],
            'total_products': 0
        }
        
        self.debug_print("Initialized URLBreacher", 'INFO')

    def debug_print(self, message, level='INFO'):
        """Enhanced debug print with timestamp and level."""
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        level_emoji = {
            'INFO': 'ℹ️',
            'SUCCESS': '✅',
            'WARNING': '⚠️',
            'ERROR': '❌',
            'STEP': '👉'
        }
        emoji = level_emoji.get(level, 'ℹ️')
        print(f"[{timestamp}] {emoji} {message}")
        
    def save_batch(self, force=False):
        """Save URLs in batches to avoid data loss."""
        if len(self.product_urls) - self.last_save_count >= self.save_batch_size or force:
            self.debug_print(f"Saving batch of URLs. Total URLs: {len(self.product_urls)}", 'INFO')
            
            # Load existing data if file exists
            existing_data = {}
            try:
                if os.path.exists(self.output_file):
                    with open(self.output_file, 'r') as f:
                        existing_data = json.load(f)
            except Exception as e:
                self.debug_print(f"Error reading existing file: {e}", 'ERROR')
                existing_data = {}
            
            # Update data
            current_data = {
                'product_urls': list(self.product_urls),
                'stats': {
                    'total_products': self.total_products_found,
                    'products_per_page': self.products_per_page,
                    'total_pages_processed': len(self.products_per_page),
                    'average_products_per_page': sum(self.products_per_page) / len(self.products_per_page) if self.products_per_page else 0,
                    'last_update': datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                }
            }
            
            # Merge with existing data
            if existing_data:
                current_data['product_urls'] = list(set(current_data['product_urls']))  # Remove duplicates
            
            # Save to file
            try:
                with open(self.output_file, 'w') as f:
                    json.dump(current_data, f, indent=4)
                self.last_save_count = len(self.product_urls)
                self.debug_print(f"Successfully saved batch to {self.output_file}", 'SUCCESS')
            except Exception as e:
                self.debug_print(f"Error saving batch: {e}", 'ERROR')

    def test_proxy(self, proxy):
        """Test if a proxy is working."""
        try:
            test_url = "http://www.google.com"
            proxies = {
                "http": f"http://{proxy}",
                "https": f"http://{proxy}"
            }
            response = requests.get(test_url, proxies=proxies, timeout=10)
            return response.status_code == 200
        except Exception:
            return False

    def initialize_proxies(self):
        """Initialize and test proxies."""
        logging.info("Fetching and testing proxies...")
        try:
            response = requests.get('https://raw.githubusercontent.com/TheSpeedX/PROXY-List/master/http.txt', timeout=10)
            if response.status_code == 200:
                proxies = [proxy.strip() for proxy in response.text.split('\n') if proxy.strip()]
                
                for proxy in proxies:
                    logging.info(f"Testing proxy: {proxy}")
                    if self.test_proxy(proxy):
                        logging.info(f"Found working proxy: {proxy}")
                        self.working_proxies.append(proxy)
                        if len(self.working_proxies) >= 5:
                            break
            
            if not self.working_proxies:
                logging.warning("No working proxies found. Will proceed without proxy.")
            else:
                self.current_proxy = self.working_proxies[0]
                
        except Exception as e:
            logging.error(f"Failed to fetch proxies: {e}")

    def get_next_proxy(self):
        """Rotate to the next working proxy."""
        if not self.working_proxies:
            return None
        
        if self.current_proxy in self.working_proxies:
            current_index = self.working_proxies.index(self.current_proxy)
            next_index = (current_index + 1) % len(self.working_proxies)
            self.current_proxy = self.working_proxies[next_index]
        else:
            self.current_proxy = self.working_proxies[0]
        
        return self.current_proxy

    def detect_site_type(self, url):
        """Detect which e-commerce site we're dealing with."""
        domain = urlparse(url).netloc.lower()
        if 'alibaba' in domain:
            return 'alibaba'
        elif 'noon' in domain:
            return 'noon'
        elif 'sharafdg' in domain:
            return 'sharafdg'
        return 'generic'

    async def scroll_to_bottom(self, page):
        """Scroll to bottom for infinite scroll pages."""
        try:
            previous_height = await page.evaluate('document.body.scrollHeight')
            while True:
                await page.evaluate('window.scrollTo(0, document.body.scrollHeight)')
                await page.wait_for_timeout(2000)  # Wait for content to load
                new_height = await page.evaluate('document.body.scrollHeight')
                if new_height == previous_height:
                    break
                previous_height = new_height
        except Exception as e:
            logging.error(f"Error during scrolling: {e}")

    def extract_urls(self, html_content, current_url):
        """Extract and categorize URLs from HTML content."""
        soup = BeautifulSoup(html_content, 'html.parser')
        site_type = self.detect_site_type(current_url)
        selectors = self.site_selectors.get(site_type, {})
        base_url = self.base_url or urlparse(current_url).scheme + "://" + urlparse(current_url).netloc
        
        # Extract product URLs using site-specific selectors
        if selectors:
            for product_selector in selectors['product']:
                products = soup.select(product_selector)
                for product in products:
                    for link_selector in selectors['link']:
                        links = product.select(link_selector)
                        for link in links:
                            url = link.get('href')
                            if url:
                                url = urljoin(base_url, url)
                                if url not in self.visited_urls:
                                    if self.is_product_url(url):
                                        self.product_urls.add(url)
                                    self.url_queue.put(url)
                                    self.visited_urls.add(url)
                                    self.stats['total_urls_found'] += 1

        # Generic URL extraction as fallback
        for link in soup.find_all('a', href=True):
            url = urljoin(base_url, link['href'])
            
            if not url.startswith(('http://', 'https://')) or url in self.visited_urls:
                continue
                
            if self.is_product_url(url):
                self.product_urls.add(url)
            elif self.is_category_url(url):
                self.category_urls.add(url)
            elif self.is_pagination_url(url):
                self.pagination_urls.add(url) 
            
            with self.url_lock:
                self.visited_urls.add(url)
                self.stats['total_urls_found'] += 1
                
            if len(self.visited_urls) <= self.max_depth:
                self.url_queue.put(url)

    def is_product_url(self, url):
        """Identify if URL is a product page using enhanced patterns."""
        patterns = [
            # Generic patterns
            r'/p/',
            r'/product/',
            r'/item/',
            r'pid=',
            r'product_id=',
            # Alibaba patterns
            r'/item/\d+',
            r'/product/\d+-\d+',
            # Noon patterns
            r'/product-p\d+',
            r'/\w+/\d+/p/',
            # Sharaf DG patterns
            r'/p/\d+',
            r'/product-details/'
        ]
        return any(re.search(pattern, url.lower()) for pattern in patterns)

    def is_category_url(self, url):
        """Identify if URL is a category page using enhanced patterns."""
        patterns = [
            # Generic patterns
            r'/c/',
            r'/category/',
            r'/department/',
            r'cat=',
            r'category_id=',
            # Site-specific patterns
            r'/catalog/',
            r'/products/',
            r'/collection/',
            r'/shop/',
            r'/deals/'
        ]
        return any(re.search(pattern, url.lower()) for pattern in patterns)

    def is_pagination_url(self, url):
        """Identify if URL is a pagination page."""
        patterns = [
            r'page=',
            r'/page/',
            r'p=\d+',
            r'offset=',
        ]
        return any(re.search(pattern, url.lower()) for pattern in patterns)

    async def handle_dynamic_content(self, page, site_type):
        """Handle dynamic content loading based on site type."""
        selectors = self.site_selectors.get(site_type, {})
        
        if selectors.get('infinite_scroll', False):
            await self.scroll_to_bottom(page)
        else:
            # Handle pagination
            next_page_selectors = selectors.get('next_page', [])
            for selector in next_page_selectors:
                try:
                    next_button = await page.query_selector(selector)
                    if next_button and await next_button.is_visible():
                        await next_button.click()
                        await page.wait_for_load_state('networkidle')
                        return True
                except Exception as e:
                    logging.error(f"Error handling pagination: {e}")
        return False

    async def extract_product_urls(self, page, site_type):
        """Extract product URLs from the current page."""
        self.debug_print(f"Starting product URL extraction for site type: {site_type}", 'STEP')
        
        try:
            # For Amazon, use a specific extraction strategy
            if site_type == 'amazon':
                self.debug_print("Using Amazon-specific extraction strategy", 'INFO')
                try:
                    # Wait for products to load
                    await page.wait_for_selector('div[data-component-type="s-search-result"]', timeout=10000)
                    
                    # Get all product containers
                    products = await page.query_selector_all('div[data-component-type="s-search-result"]')
                    self.debug_print(f"Found {len(products)} product containers", 'INFO')
                    
                    product_count = 0
                    for product in products:
                        try:
                            # Try to get the product link
                            link_element = await product.query_selector('h2 a.a-link-normal')
                            if not link_element:
                                link_element = await product.query_selector('a[href*="/dp/"]')
                            
                            if link_element:
                                url = await link_element.get_attribute('href')
                                if url:
                                    # Clean and validate the URL
                                    full_url = urljoin(self.base_url, url)
                                    # Extract the product ID (ASIN)
                                    asin_match = re.search(r'/dp/([A-Z0-9]{10})', full_url)
                                    if asin_match:
                                        # Standardize Amazon URL format
                                        clean_url = f"https://www.amazon.in/dp/{asin_match.group(1)}"
                                        self.product_urls.add(clean_url)
                                        product_count += 1
                                        
                                        if product_count % 5 == 0:  # Log every 5 products
                                            self.debug_print(f"Extracted {product_count} products so far", 'SUCCESS')
                        except Exception as e:
                            continue
                    
                    self.debug_print(f"Successfully extracted {product_count} Amazon product URLs", 'SUCCESS')
                    return product_count
                    
                except Exception as e:
                    self.debug_print(f"Amazon-specific extraction failed: {e}", 'ERROR')
                    return 0
            
            # For other sites, use the general extraction logic
            return await self._general_extract_product_urls(page, site_type)
            
        except Exception as e:
            self.debug_print(f"Error in product URL extraction: {e}", 'ERROR')
            return 0

    async def handle_pagination(self, page, site_type):
        """Handle pagination with improved error handling and task management."""
        try:
            self.debug_print("Attempting pagination...", 'STEP')
            
            if site_type == 'amazon':
                try:
                    # Wait for the pagination button
                    next_button = await page.wait_for_selector('.s-pagination-next:not([aria-disabled="true"])', timeout=5000)
                    
                    if next_button:
                        # Check if the button is visible and enabled
                        is_visible = await next_button.is_visible()
                        if is_visible:
                            self.debug_print("Found active next page button", 'SUCCESS')
                            await next_button.click()
                            await page.wait_for_load_state('networkidle')
                            
                            # Verify page changed
                            await page.wait_for_selector('div[data-component-type="s-search-result"]', timeout=10000)
                            self.debug_print("Successfully navigated to next page", 'SUCCESS')
                            return True
                    else:
                        self.debug_print("No more pages available", 'INFO')
                        return False
                        
                except Exception as e:
                    self.debug_print(f"Amazon pagination failed: {e}", 'ERROR')
                    
                    # Fallback: Try URL modification
                    try:
                        current_url = page.url
                        current_page = re.search(r'page=(\d+)', current_url)
                        if current_page:
                            next_page = int(current_page.group(1)) + 1
                            next_url = re.sub(r'page=\d+', f'page={next_page}', current_url)
                        else:
                            if '?' in current_url:
                                next_url = current_url + '&page=2'
                            else:
                                next_url = current_url + '?page=2'
                        
                        await page.goto(next_url, wait_until='networkidle')
                        await page.wait_for_selector('div[data-component-type="s-search-result"]', timeout=10000)
                        self.debug_print("Successfully navigated to next page via URL modification", 'SUCCESS')
                        return True
                    except Exception as e:
                        self.debug_print(f"URL pagination fallback failed: {e}", 'ERROR')
                        return False
            
            # For other sites, use the general pagination logic
            return await self._general_handle_pagination(page, site_type)
            
        except Exception as e:
            self.debug_print(f"Error during pagination: {e}", 'ERROR')
            return False

    async def _init_playwright(self):
        """Initialize Playwright browser."""
        try:
            self.debug_print("Initializing Playwright browser...", 'STEP')
            playwright = await async_playwright().start()
            browser = await playwright.chromium.launch(
                headless=True,
                args=['--disable-dev-shm-usage', '--no-sandbox']
            )
            context = await browser.new_context(
                viewport={'width': 1920, 'height': 1080},
                user_agent=UserAgent().random
            )
            page = await context.new_page()
            
            # Set default timeouts
            await page.set_default_timeout(30000)
            await page.set_default_navigation_timeout(30000)
            
            # Enable JavaScript
            await context.add_init_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
            
            self.debug_print("Playwright browser initialized successfully", 'SUCCESS')
            return playwright, browser, context, page
        except Exception as e:
            self.debug_print(f"Failed to initialize Playwright: {e}", 'ERROR')
            raise

    async def create_browser(self, playwright):
        """Create a browser instance with enhanced stealth."""
        try:
            browser_args = [
                '--disable-blink-features=AutomationControlled',
                '--disable-features=IsolateOrigins,site-per-process',
                '--disable-site-isolation-trials',
                '--disable-web-security',
                '--disable-dev-shm-usage',
                '--no-sandbox',
                '--disable-setuid-sandbox',
                '--disable-gpu',
                '--disable-infobars',
                '--window-size=1920,1080',
                f'--proxy-server={self.current_proxy}' if self.current_proxy else '',
                '--user-agent=' + self.user_agent
            ]

            browser = await playwright.chromium.launch(
                headless=True,
                args=browser_args,
                ignore_default_args=['--enable-automation']
            )
            return browser
        except Exception as e:
            self.debug_print(f"Error creating browser: {e}", 'ERROR')
            raise

    async def create_page(self, browser):
        """Create a page with enhanced anti-detection measures."""
        try:
            context = await browser.new_context(
                viewport={'width': 1920, 'height': 1080},
                user_agent=self.user_agent,
                ignore_https_errors=True
            )

            # Add stealth scripts
            await context.add_init_script("""
                Object.defineProperty(navigator, 'webdriver', {get: () => undefined});
                Object.defineProperty(navigator, 'plugins', {get: () => [1, 2, 3, 4, 5]});
                Object.defineProperty(navigator, 'languages', {get: () => ['en-US', 'en']});
                window.chrome = { runtime: {} };
            """)

            page = await context.new_page()
            
            # Additional page configurations
            await page.set_extra_http_headers({
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
                'Accept-Language': 'en-US,en;q=0.5',
                'Cache-Control': 'no-cache',
                'Pragma': 'no-cache',
            })

            return page
        except Exception as e:
            self.debug_print(f"Error creating page: {e}", 'ERROR')
            raise

    async def attempt_breach(self, url, max_retries=3):
        """Attempt to breach the website with multiple strategies."""
        self.debug_print("=== Starting Website Breach Attempt ===", 'STEP')
        for attempt in range(max_retries):
            try:
                self.debug_print(f"Breach Attempt {attempt + 1}/{max_retries}", 'STEP')
                
                # Initialize Playwright
                playwright = await async_playwright().start()
                browser = await playwright.chromium.launch(
                    headless=True,
                    args=['--disable-dev-shm-usage', '--no-sandbox']
                )
                context = await browser.new_context(
                    viewport={'width': 1920, 'height': 1080},
                    user_agent=UserAgent().random
                )
                page = await context.new_page()
                
                # Set default timeouts
                await page.set_default_timeout(30000)
                await page.set_default_navigation_timeout(30000)
                
                try:
                    # Strategy 1: Direct access with stealth
                    self.debug_print("Attempting direct access with stealth...", 'STEP')
                    await context.add_init_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
                    await page.goto(url, wait_until='networkidle')
                    content = await page.content()
                    if len(content) > 1000:
                        self.debug_print("Direct access successful!", 'SUCCESS')
                        return page, browser, context, playwright
                except Exception as e:
                    self.debug_print(f"Direct access failed: {str(e)}", 'ERROR')
                
                try:
                    # Strategy 2: Try with aiohttp
                    self.debug_print("Attempting aiohttp method...", 'STEP')
                    async with aiohttp.ClientSession() as session:
                        async with session.get(url, headers={'User-Agent': UserAgent().random}) as response:
                            if response.status == 200:
                                content = await response.text()
                                await page.set_content(content)
                                self.debug_print("Aiohttp access successful!", 'SUCCESS')
                                return page, browser, context, playwright
                except Exception as e:
                    self.debug_print(f"Aiohttp failed: {str(e)}", 'ERROR')
                
                try:
                    # Strategy 3: Try with different user agent and proxy
                    self.debug_print("Attempting user agent and proxy rotation...", 'STEP')
                    await context.clear_cookies()
                    await context.new_context(
                        user_agent=UserAgent().random,
                        proxy={
                            'server': 'http://proxy-server.scraperapi.com:8001',
                            'username': 'scraperapi',
                            'password': 'free'
                        }
                    )
                    await page.goto(url, wait_until='networkidle')
                    content = await page.content()
                    if len(content) > 1000:
                        self.debug_print("User agent and proxy rotation successful!", 'SUCCESS')
                        return page, browser, context, playwright
                except Exception as e:
                    self.debug_print(f"User agent and proxy rotation failed: {str(e)}", 'ERROR')
                
                # Clean up if all strategies fail
                await context.close()
                await browser.close()
                await playwright.stop()
                
                self.debug_print(f"All strategies failed for attempt {attempt + 1}", 'WARNING')
                
                # Wait between attempts
                if attempt < max_retries - 1:
                    delay = random.uniform(2, 5)
                    self.debug_print(f"Waiting {delay:.1f} seconds before next attempt...")
                    await asyncio.sleep(delay)
            
            except Exception as e:
                self.debug_print(f"Critical error in breach attempt {attempt + 1}: {str(e)}", 'ERROR')
                try:
                    await context.close()
                    await browser.close()
                    await playwright.stop()
                except:
                    pass
        
        self.debug_print("All breach attempts failed!", 'ERROR')
        raise Exception("Failed to breach website after all attempts")

    async def scrape_url(self, url):
        """Main method to scrape a URL with enhanced fallback mechanisms."""
        self.debug_print("\n=== Starting URL Scraping Process ===", 'STEP')
        self.stats['start_time'] = datetime.now()
        site_type = self.detect_site_type(url)
        self.debug_print(f"Detected site type: {site_type}", 'INFO')
        
        try:
            # Create event loop if needed
            try:
                self.debug_print("Setting up event loop...")
                loop = asyncio.get_event_loop()
            except RuntimeError:
                self.debug_print("No event loop found, creating new one...")
                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)
            
            # Attempt to breach the website
            self.debug_print(f"Starting breach attempt for {url}...", 'STEP')
            page, browser, context, playwright = await self.attempt_breach(url)
            
            try:
                self.debug_print("Website successfully breached!", 'SUCCESS')
                
                # Get initial counts with fallback
                self.debug_print("Analyzing page structure...", 'STEP')
                total_pages = await self.count_total_pages(page, site_type) or 1
                total_products = await self.estimate_total_products(page, site_type)
                
                self.debug_print(f"""
                📊 Category Overview:
                ----------------------
                Site Type: {site_type}
                Total Pages: {total_pages}
                Estimated Products: {total_products if total_products else 'Unknown'}
                ----------------------
                """, 'INFO')
                
                # Extract URLs with multiple fallback methods
                current_page = 1
                while current_page <= total_pages:
                    self.debug_print(f"\n=== Processing Page {current_page}/{total_pages} ===", 'STEP')
                    
                    # Try multiple extraction methods
                    self.debug_print("Attempting primary URL extraction...")
                    products_found = await self.extract_product_urls(page, site_type)
                    
                    if products_found == 0:
                        self.debug_print("Primary extraction failed, trying generic selectors...", 'WARNING')
                        products_found = await self.extract_product_urls(page, 'generic')
                    
                    if products_found == 0:
                        self.debug_print("Generic extraction failed, trying raw link extraction...", 'WARNING')
                        all_links = await page.query_selector_all('a')
                        self.debug_print(f"Found {len(all_links)} raw links to analyze", 'INFO')
                        for link in all_links:
                            try:
                                url = await link.get_attribute('href')
                                if url and self.is_product_url(url):
                                    full_url = urljoin(self.base_url, url)
                                    self.product_urls.add(full_url)
                                    products_found += 1
                            except Exception as e:
                                continue
                    
                    self.debug_print(f"Found {products_found} products on page {current_page}", 'SUCCESS' if products_found > 0 else 'WARNING')
                    
                    # Save progress
                    if products_found > 0:
                        self.debug_print("Saving current batch of URLs...")
                        self.save_batch()
                    
                    # Handle pagination with fallback
                    if current_page < total_pages:
                        self.debug_print(f"\n=== Attempting Pagination to Page {current_page + 1} ===", 'STEP')
                        pagination_task = asyncio.create_task(self.handle_pagination(page, site_type))
                        success = await pagination_task
                        
                        if not success:
                            self.debug_print("Primary pagination failed, trying generic pagination...", 'WARNING')
                            pagination_task = asyncio.create_task(self.handle_pagination(page, 'generic'))
                            success = await pagination_task
                        
                        if success:
                            self.debug_print("Pagination successful!", 'SUCCESS')
                            current_page += 1
                            await page.wait_for_load_state('networkidle')
                            delay = random.uniform(1, 3)
                            self.debug_print(f"Waiting {delay:.1f} seconds before processing next page...")
                            await asyncio.sleep(delay)
                        else:
                            self.debug_print("All pagination attempts failed, stopping.", 'ERROR')
                            break
                
            finally:
                self.debug_print("Closing browser...")
                await browser.close()
                
        except Exception as e:
            self.debug_print(f"Critical error during scrape: {str(e)}", 'ERROR')
            logging.error(f"Error during crawl: {e}")
            self.stats['failed_scrapes'] += 1
        finally:
            self.stats['end_time'] = datetime.now()
            duration = self.stats['end_time'] - self.stats['start_time']
            self.debug_print(f"""
            === Final Scraping Summary ===
            Duration: {duration}
            Total Products Found: {len(self.product_urls)}
            Total Pages Processed: {len(self.products_per_page)}
            Average Products/Page: {sum(self.products_per_page) / len(self.products_per_page):.1f}
            """, 'INFO')
            self._print_final_stats()
            self.save_batch(force=True)

    async def crawl(self, url):
        """Crawl a URL and extract product URLs."""
        start_time = datetime.now()
        try:
            self.debug_print(f"Starting crawl for {url}...", 'STEP')
            logging.info(f"Starting crawl for {url}")
            
            # Attempt to breach the website
            self.debug_print(f"Starting breach attempt for {url}...", 'STEP')
            page, browser, context, playwright = await self.attempt_breach(url)
            
            try:
                self.debug_print("Website successfully breached!", 'SUCCESS')
                
                # Get the site type based on URL
                site_type = self.detect_site_type(url)
                self.debug_print(f"Detected site type: {site_type}", 'INFO')
                
                # Extract URLs from the current page
                products_found = await self.extract_product_urls(page, site_type)
                self.debug_print(f"Found {products_found} products on initial page", 'SUCCESS')
                
                # Handle pagination if products were found
                if products_found > 0:
                    page_count = 1
                    while page_count < self.max_pages:
                        self.debug_print(f"Attempting pagination...", 'STEP')
                        if not await self.handle_pagination(page, site_type):
                            self.debug_print("No more pages to process", 'INFO')
                            break
                        
                        # Wait for content to load after pagination
                        await page.wait_for_load_state('networkidle')
                        
                        # Extract URLs from the new page
                        new_products = await self.extract_product_urls(page, site_type)
                        if new_products == 0:
                            self.debug_print("No new products found, stopping pagination", 'WARNING')
                            break
                            
                        page_count += 1
                        self.debug_print(f"Processed page {page_count}", 'SUCCESS')
                
                self.debug_print(f"Processed {page_count} pages", 'INFO')
                return True
                
            except Exception as e:
                self.debug_print(f"Error during crawl: {e}", 'ERROR')
                logging.error(f"Error during crawl: {e}")
                return False
                
            finally:
                # Clean up
                await context.close()
                await browser.close()
                await playwright.stop()
                
        except Exception as e:
            self.debug_print(f"Error during crawl: {e}", 'ERROR')
            logging.error(f"Error during crawl: {e}")
            return False
            
        finally:
            duration = datetime.now() - start_time
            self.debug_print(f"""
            Final Statistics:
            - Duration: {duration}
            - Total URLs found: {len(self.product_urls)}
            - Successful scrapes: {len(self.products_per_page)}
            - Failed scrapes: {self.failed_scrapes}
            - Product URLs found: {len(self.product_urls)}
            - Category URLs found: {len(self.category_urls)}
            """, 'INFO')
            self._print_final_stats()
            self.save_batch(force=True)

    def scrape_with_playwright(self, url):
        """Enhanced Playwright scraping focused on product URL extraction."""
        logging.info("Starting Playwright scraping...")
        
        try:
            with sync_playwright() as p:
                browser = p.chromium.launch(
                    headless=True,
                    args=['--no-sandbox', '--disable-setuid-sandbox']
                )
                
                context = browser.new_context(
                    user_agent=self.user_agent,
                    viewport={'width': 1920, 'height': 1080},
                    java_script_enabled=True
                )
                
                page = context.new_page()
                page.set_default_timeout(30000)
                
                # Handle bot detection
                page.route("**/*", lambda route: route.continue_() 
                         if route.request.resource_type in ['document', 'script', 'xhr', 'fetch']
                         else route.abort())
                
                logging.info(f"Navigating to {url}")
                response = page.goto(url, wait_until="networkidle", timeout=60000)
                
                if response.status >= 400:
                    raise Exception(f"HTTP {response.status} error")
                
                # Handle common overlays
                for selector in ['button[id*="cookie"]', 'button[class*="popup"]', 'div[class*="overlay"]']:
                    try:
                        page.click(selector, timeout=5000)
                    except:
                        pass
                
                site_type = self.detect_site_type(url)
                pages_processed = asyncio.get_event_loop().run_until_complete(
                    self.handle_pagination(page, site_type)
                )
                
                logging.info(f"Processed {pages_processed} pages")
                logging.info(f"Total product URLs found: {len(self.product_urls)}")
                
                browser.close()
                return True
                
        except Exception as e:
            logging.error(f"Playwright scraping error: {e}")
            raise

    def scrape_with_selenium(self, url):
        """Scrape using Selenium with undetected chromedriver."""
        last_error = None
        max_retries = 3
        
        for attempt in range(max_retries):
            options = Options()
            options.add_argument(f"user-agent={self.user_agent}")
            options.add_argument("--headless")
            options.add_argument("--disable-gpu")
            options.add_argument("--no-sandbox")
            options.add_argument("--disable-dev-shm-usage")

            if self.current_proxy:
                options.add_argument(f"--proxy-server=http://{self.current_proxy}")

            try:
                service = Service(ChromeDriverManager().install())
                driver = webdriver.Chrome(service=service, options=options)
                driver.set_page_load_timeout(30)
                
                # Wait for page load and dynamic content
                driver.get(url)
                WebDriverWait(driver, 10).until(
                    EC.presence_of_element_located((By.TAG_NAME, "body"))
                )
                
                # Scroll to load lazy content
                driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                time.sleep(3)
                
                content = driver.page_source
                driver.quit()
                
                self.stats['successful_scrapes'] += 1
                return content
            except Exception as e:
                last_error = e
                logging.error(f"Attempt {attempt + 1} failed with proxy {self.current_proxy}")
                driver.quit() if 'driver' in locals() else None
                self.current_proxy = self.get_next_proxy()
                self.stats['failed_scrapes'] += 1
                if not self.current_proxy:
                    logging.warning("No more proxies available. Trying without proxy...")
                    break

        # Try one last time without proxy if all proxy attempts failed
        if last_error:
            try:
                options = Options()
                options.add_argument(f"user-agent={self.user_agent}")
                options.add_argument("--headless")
                options.add_argument("--disable-gpu")
                options.add_argument("--no-sandbox")
                options.add_argument("--disable-dev-shm-usage")
                
                service = Service(ChromeDriverManager().install())
                driver = webdriver.Chrome(service=service, options=options)
                driver.set_page_load_timeout(30)
                driver.get(url)
                time.sleep(3)
                content = driver.page_source
                driver.quit()
                
                self.stats['successful_scrapes'] += 1
                return content
            except Exception as e:
                logging.error(f"Final attempt without proxy also failed: {e}")
                self.stats['failed_scrapes'] += 1
                raise

    def scrape_with_cloudscraper(self, url):
        """Scrape using cloudscraper to bypass Cloudflare."""
        try:
            scraper = cloudscraper.create_scraper()
            response = scraper.get(url)
            content = response.text
            self.extract_urls(content, url)
            self.stats['successful_scrapes'] += 1
            return content
        except Exception as e:
            logging.error(f"Cloudscraper failed: {e}")
            self.stats['failed_scrapes'] += 1
            raise

    def scrape(self, url):
        """Dynamically choose the best scraping approach."""
        if self.is_cloudflare(url):
            logging.info(f"Cloudflare detected on {url}, using cloudscraper.")
            return self.scrape_with_cloudscraper(url)
        else:
            try:
                logging.info(f"Trying Playwright for {url}")
                return self.scrape_with_playwright(url)
            except Exception as e:
                logging.error(f"Playwright failed: {e}. Falling back to Selenium.")
                return self.scrape_with_selenium(url)

    def is_cloudflare(self, url):
        """Detect if a website is protected by Cloudflare."""
        try:
            scraper = cloudscraper.create_scraper()
            response = scraper.get(url)
            return "cloudflare" in response.text.lower()
        except Exception as e:
            logging.error(f"Cloudflare detection failed: {e}")
            return False

    def crawl(self, start_url=None):
        """Start crawling from a category URL."""
        if not start_url:
            logging.error("No start URL provided")
            return
            
        self.base_url = urlparse(start_url).scheme + "://" + urlparse(start_url).netloc
        self.stats['start_time'] = datetime.now()
        
        logging.info(f"\n{'='*50}\nStarting crawl for category: {start_url}\n{'='*50}")
        
        try:
            success = self.scrape_with_playwright(start_url)
            if success:
                logging.info("Successfully scraped category page")
            else:
                logging.error("Failed to scrape category page")
                
        except KeyboardInterrupt:
            logging.info("\nCrawling interrupted by user")
        except Exception as e:
            logging.error(f"Error during crawl: {e}")
        finally:
            self.stats['end_time'] = datetime.now()
            self._print_final_stats()
            self.save_batch(force=True)
            self._save_progress()

    def _print_progress(self):
        """Print current progress statistics."""
        logging.info(f"""
        Progress Update:
        - Total URLs found: {self.stats['total_urls_found']}
        - Successful scrapes: {self.stats['successful_scrapes']}
        - Failed scrapes: {self.stats['failed_scrapes']}
        - Product URLs found: {len(self.product_urls)}
        - Category URLs found: {len(self.category_urls)}
        - Queue size: {self.url_queue.qsize()}
        """)
        
    def _print_final_stats(self):
        """Print final statistics."""
        duration = self.stats['end_time'] - self.stats['start_time']
        logging.info(f"""
        Final Statistics:
        - Duration: {duration}
        - Total URLs found: {self.stats['total_urls_found']}
        - Successful scrapes: {self.stats['successful_scrapes']}
        - Failed scrapes: {self.stats['failed_scrapes']}
        - Product URLs found: {len(self.product_urls)}
        - Category URLs found: {len(self.category_urls)}
        """)

    def _save_progress(self):
        """Save current progress to file."""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        data = {
            'product_urls': list(self.product_urls),
            'category_urls': list(self.category_urls),
            'pagination_urls': list(self.pagination_urls),
            'stats': {
                'total_urls': len(self.visited_urls),
                'successful_scrapes': self.stats['successful_scrapes'],
                'failed_scrapes': self.stats['failed_scrapes']
            }
        }
        
        filename = f'scraping_progress_{timestamp}.json'
        with open(filename, 'w') as f:
            json.dump(data, f, indent=4)
        logging.info(f"Progress saved to {filename}")

    async def _general_extract_product_urls(self, page, site_type):
        """General method for extracting product URLs from any site."""
        selectors = self.site_selectors.get(site_type, {})
        product_urls = set()
        page_product_count = 0
        
        try:
            # First try site-specific selectors
            if selectors:
                self.debug_print(f"Using site-specific selectors for {site_type}", 'INFO')
                for product_selector in selectors['product']:
                    try:
                        products = await page.query_selector_all(product_selector)
                        self.debug_print(f"Found {len(products)} potential products with selector: {product_selector}", 'INFO')
                        
                        for product in products:
                            for link_selector in selectors['link']:
                                try:
                                    link = await product.query_selector(link_selector)
                                    if link:
                                        url = await link.get_attribute('href')
                                        if url:
                                            full_url = urljoin(self.base_url, url)
                                            if self.is_product_url(full_url):
                                                product_urls.add(full_url)
                                                page_product_count += 1
                                                self.total_products_found += 1
                                                
                                                if self.total_products_found % 10 == 0:
                                                    self.debug_print(f"Milestone: Found {self.total_products_found} total products", 'SUCCESS')
                                                    self.save_batch()
                                except Exception as e:
                                    continue
                    except Exception as e:
                        self.debug_print(f"Error with selector {product_selector}: {e}", 'ERROR')
                        continue
            
            # Fallback to generic link extraction if no products found
            if not product_urls:
                self.debug_print("No products found with specific selectors, trying generic extraction", 'WARNING')
                all_links = await page.query_selector_all('a')
                self.debug_print(f"Found {len(all_links)} total links to analyze", 'INFO')
                
                for link in all_links:
                    try:
                        url = await link.get_attribute('href')
                        if url:
                            full_url = urljoin(self.base_url, url)
                            if self.is_product_url(full_url):
                                product_urls.add(full_url)
                                page_product_count += 1
                                self.total_products_found += 1
                                
                                if self.total_products_found % 10 == 0:
                                    self.debug_print(f"Milestone: Found {self.total_products_found} total products", 'SUCCESS')
                                    self.save_batch()
                    except Exception as e:
                        continue
            
            self.product_urls.update(product_urls)
            self.products_per_page.append(page_product_count)
            
            self.debug_print(f"""
            Page Statistics:
            - Products found on this page: {page_product_count}
            - Total products found so far: {self.total_products_found}
            - Average products per page: {sum(self.products_per_page) / len(self.products_per_page):.2f}
            """, 'INFO')
            
            return page_product_count
            
        except Exception as e:
            self.debug_print(f"Error in general product URL extraction: {e}", 'ERROR')
            return 0

    async def _general_handle_pagination(self, page, site_type):
        """General method for handling pagination on any site."""
        try:
            # Get pagination selectors
            selectors = self.site_selectors.get(site_type, {})
            next_page_selectors = selectors.get('next_page', [])
            
            # Try clicking next page button
            for selector in next_page_selectors:
                try:
                    next_button = await page.query_selector(selector)
                    if next_button and await next_button.is_visible():
                        # Check if button is enabled/clickable
                        is_disabled = await next_button.get_attribute('disabled')
                        if not is_disabled:
                            await next_button.click()
                            await page.wait_for_load_state('networkidle')
                            self.debug_print("Successfully clicked next page button", 'SUCCESS')
                            return True
                except Exception as e:
                    self.debug_print(f"Click pagination failed for selector {selector}: {e}", 'ERROR')
                    continue
            
            # Fallback: Try URL pattern modification
            try:
                current_url = page.url
                parsed_url = urlparse(current_url)
                query_params = dict(parse_qsl(parsed_url.query))
                
                # Common pagination parameters
                page_params = ['page', 'p', 'pg', 'pageNumber', 'pageNum']
                
                for param in page_params:
                    if param in query_params:
                        current_page = int(query_params[param])
                        query_params[param] = str(current_page + 1)
                        new_query = urlencode(query_params)
                        new_url = parsed_url._replace(query=new_query).geturl()
                        
                        await page.goto(new_url, wait_until='networkidle')
                        self.debug_print("Successfully navigated to next page via URL modification", 'SUCCESS')
                        return True
                
            except Exception as e:
                self.debug_print(f"URL pattern pagination failed: {e}", 'ERROR')
            
            # Fallback: Try infinite scroll
            if selectors.get('infinite_scroll', False):
                try:
                    # Scroll to bottom
                    old_height = await page.evaluate('document.body.scrollHeight')
                    await page.evaluate('window.scrollTo(0, document.body.scrollHeight)')
                    await page.wait_for_timeout(2000)  # Wait for potential content load
                    
                    # Check if new content loaded
                    new_height = await page.evaluate('document.body.scrollHeight')
                    if new_height > old_height:
                        self.debug_print("Successfully loaded more content via infinite scroll", 'SUCCESS')
                        return True
                except Exception as e:
                    self.debug_print(f"Infinite scroll pagination failed: {e}", 'ERROR')
            
            self.debug_print("No pagination method succeeded", 'WARNING')
            return False
            
        except Exception as e:
            self.debug_print(f"Error in general pagination handling: {e}", 'ERROR')
            return False

class timeout:
    """Context manager for timeout."""
    def __init__(self, seconds):
        self.seconds = seconds

    def __enter__(self):
        def signal_handler(signum, frame):
            raise TimeoutError("Timed out!")
        signal.signal(signal.SIGALRM, signal_handler)
        signal.alarm(self.seconds)

    def __exit__(self, type, value, traceback):
        signal.alarm(0)

def main():
    print("=== URL Breacher - Advanced Web Scraping Tool ===")
    while True:
        url = input("\nEnter the URL to scrape (or 'quit' to exit): ")
        if url.lower() == 'quit':
            break
            
        max_depth = input("Enter maximum crawling depth (default is 3): ")
        max_depth = int(max_depth) if max_depth.isdigit() else 3
        
        breacher = URLBreacher(max_depth=max_depth)
        try:
            print("\nStarting scraping process...")
            breacher.crawl(url)
            print("\nScraping completed successfully!")
            print("\nResults have been saved to files.")
            
            # Display quick statistics
            print("\nQuick Statistics:")
            print(f"Total URLs found: {breacher.stats['total_urls_found']}")
            print(f"Successful scrapes: {breacher.stats['successful_scrapes']}")
            print(f"Failed scrapes: {breacher.stats['failed_scrapes']}")
            print(f"Product URLs found: {len(breacher.product_urls)}")
            print(f"Category URLs found: {len(breacher.category_urls)}")
            print(f"Pagination URLs found: {len(breacher.pagination_urls)}")
            
        except Exception as e:
            print(f"\nAn error occurred: {e}")

if __name__ == "__main__":
    main()
