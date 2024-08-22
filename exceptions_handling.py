import re
import requests
from common import CustomLogger, Utils

class RequestsHandling:
    """
    Handles different types of exceptions that may occur during HTTP requests.
    """

    def __init__(self, url_site, url):
        """
        Initializes the RequestsHandling object with the given URL and URL site.

        Args:
            url_site (str): The base URL of the site.
            url (str): The URL to be accessed.
            retries (int): The number of retry attempts for the request.
            timeout (int): The timeout duration for the request in seconds.
        """
        self.url = url
        self.url_site = url_site
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36',
            'Accept-Language': 'en-US,en;q=0.9,bg;q=0.8',
        }

        self.exception_handlers = {
            requests.exceptions.ConnectTimeout: self.handle_connect_timeout,
            requests.exceptions.MissingSchema: self.handle_missing_schema,
            requests.exceptions.ConnectionError: self.handle_name_resolution_error,
        }

        self.logger = CustomLogger()
        self.site_name = Utils.extract_site_name(url_site)

    def handle_connect_timeout(self):
        """
        Handles the ConnectTimeout exception.

        Args:
            url (str): The URL to connect to.

        Returns:
            tuple: A tuple containing the response object and the URL.
        """
        try:
            response = requests.get(self.url, headers=self.headers, timeout=30)
            return response, None
        except requests.exceptions.RequestException as e:
            self.logger.log("Request exception",
                            level='CRITICAL',
                            site=self.site_name,
                            exception=e)
            return None, None
    
    def handle_missing_schema(self):
        """
        Handles the MissingSchema exception.

        Returns:
            tuple: A tuple containing the response object and the full URL.
        """
        patterns = [
            re.compile(r"(https?://[^/]+?)(?=/)"),
            re.compile(r"(https?://[^/]+?)(?=/|$)")
        ]

        domain = None
        for pattern in patterns:
            match = re.match(pattern, self.url_site)
            if match:
                domain = match.group(1)
                break
        if domain and self.url:
            domain_without_protocol = domain.replace("https://", "").replace("http://", "").replace("www.", "")
            url_without_protocol = self.url.replace("https://", "").replace("http://", "").replace("www.", "")
            
            if self.url.startswith("//"):
                full_url = "https:" + self.url
            elif self.url.startswith("/"):
                full_url = domain + self.url
            elif domain_without_protocol in url_without_protocol:
                full_url = self.url if self.url.startswith("http") else "https://" + self.url
            else:
                full_url = domain + "/" + self.url
            if full_url.startswith("https:////"):
                full_url = full_url.replace("https:////", "https://")
            if full_url.startswith("https:///"):
                full_url = full_url.replace("https:///", "https://")
            try:
                response = requests.get(full_url, headers=self.headers)
                return response, full_url
            except requests.exceptions.RequestException as e:
                self.logger.log("Request exception",
                                level='CRITICAL',
                                site=self.site_name,
                                exception=e)
                return None, full_url

    def handle_name_resolution_error(self):
        """
        Handles the ConnectionError (NameResolutionError) exception.

        Returns:
            tuple: A tuple containing the response object and None.
        """
        try:
            response = requests.get(self.url, headers=self.headers)
            return response, None
        except requests.exceptions.RequestException as e:
            self.logger.log("Request exception",
                            level='CRITICAL',
                            site=self.site_name,
                            exception=e)
            return None, None

    def main(self):
        """
        Executes the main functionality of the RequestsHandling class.

        Returns:
            tuple: A tuple containing the response object and the URL if successful, otherwise (None, None).
        """
        retries = 3

        for _ in range(retries):
            try:
                response = requests.get(self.url, timeout=10)
                if response.ok:
                    return response, self.url
            except Exception as e:
                handled = False
                for exception_type, handler_func in self.exception_handlers.items():
                    if isinstance(e, exception_type):
                        response, new_url = handler_func()
                        if response and response.ok:
                            handled = True
                            self.url = new_url
                            return response, self.url
                        elif new_url is not None:
                            self.url = new_url
                            handled = True
                            break
                if not handled:
                    return None, self.url
        self.logger.log(f"All {retries} attempts failed for URL: {self.url}",
                        level='CRITICAl',
                        site=self.site_name)
        return None, self.url
