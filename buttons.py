import time

from selenium.common.exceptions import NoSuchElementException, TimeoutException, WebDriverException, ElementNotInteractableException
from selenium.webdriver.support.wait import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By


from common import CustomLogger, Jsons


class InteractWithButtons:
    """
    Interacts with various buttons on a webpage.
    """

    def __init__(self, driver, site_name):
        """
        Initializes the InteractWithButtons object.

        Args:
            driver: Selenium WebDriver instance.
            site_name (str): Name of the site being interacted with.
        """
        self.driver = driver
        self.site_name = site_name
        self.logger = CustomLogger()
        self.xpaths = Jsons.load_configs(self.site_name)

    def enter_button(self):
        """
        Interact with enter button.
        """
        enter_btt_xpaths = self.xpaths.get("enter_button", [])
        if enter_btt_xpaths == [""]:
            self.logger.log("No defined enter button xpaths",
                            level='ERROR', 
                            site=self.site_name)
            return None
        if not enter_btt_xpaths:
            return None

        for xpath in enter_btt_xpaths:
            try:
                enter = WebDriverWait(self.driver, 15).until(
                    EC.presence_of_element_located((By.XPATH, xpath)))
                self.driver.execute_script(
                    "arguments[0].scrollIntoView();", enter)
                time.sleep(0.5)
                self.driver.execute_script("arguments[0].click();", enter)
                self.logger.log("Entered the site", level='INFO', site=self.site_name)
                break
            except NoSuchElementException as nse_error:
                self.logger.log("No enter button",
                                level='ERROR',
                                site=self.site_name,
                                exception=nse_error)
                continue
            except TimeoutException as timeout_error:
                self.logger.log("Enter button timed out",
                                level='ERROR',
                                site=self.site_name,
                                exception=timeout_error)
                continue
        return None

    def second_enter_button(self):
        """
        Interact with enter button.
        """
        enter_btt_xpaths = self.xpaths.get("second_enter_button", [])
        if enter_btt_xpaths == [""]:
            self.logger.log("No defined second enter button xpaths",
                            level='ERROR', 
                            site=self.site_name)
            return None
        if not enter_btt_xpaths:
            return None

        for xpath in enter_btt_xpaths:
            try:
                enter = WebDriverWait(self.driver, 15).until(
                    EC.presence_of_element_located((By.XPATH, xpath)))
                self.driver.execute_script(
                    "arguments[0].scrollIntoView();", enter)
                time.sleep(0.5)
                self.driver.execute_script("arguments[0].click();", enter)
                self.logger.log("Entered the site", level='INFO', site=self.site_name)
                break
            except NoSuchElementException as nse_error:
                self.logger.log("No enter button",
                                level='ERROR',
                                site=self.site_name,
                                exception=nse_error)
                continue
            except TimeoutException as timeout_error:
                self.logger.log("Enter button timed out",
                                level='ERROR',
                                site=self.site_name,
                                exception=timeout_error)
                continue
        return None

    def click_video(self):
        """
        Interact with video.
        """
        video_btt_xpaths = self.xpaths.get("video_button", [])
        if video_btt_xpaths == [""]:
            self.logger.log("No defined video button xpaths",
                            level='ERROR', 
                            site=self.site_name)
            return None
        if not video_btt_xpaths:
            return None

        for xpath in video_btt_xpaths:
            try:
                video = WebDriverWait(self.driver, 10).until(
                    EC.presence_of_element_located((By.XPATH, xpath)))
                try:
                    video.click()
                except WebDriverException:
                    self.driver.execute_script(
                        "arguments[0].scrollIntoView();", video)
                    time.sleep(0.5)
                    self.driver.execute_script("arguments[0].click();", video)
                    time.sleep(3)
                self.logger.log("Video clicked", level='INFO', site=self.site_name)
                break
            except NoSuchElementException as nse_error:
                self.logger.log("No video button",
                                level='ERROR',
                                site=self.site_name,
                                exception=nse_error)
                continue
            except TimeoutException as timeout_error:
                self.logger.log("Video button timed out",
                                level='ERROR',
                                site=self.site_name,
                                exception=timeout_error)
                continue
            except ElementNotInteractableException as eni_error:
                self.logger.log("Video button is not interactable",
                                level='ERROR',
                                site=self.site_name,
                                exception=eni_error)
                continue
        return None

    def expand_desc_button(self):
        """
        Interact with expand description button.
        """
        expand_btt_xpaths = self.xpaths.get("expand_desc_button", [])
        if expand_btt_xpaths == [""]:
            self.logger.log("No defined expand description button xpaths",
                            level='ERROR', 
                            site=self.site_name)
            return None
        if not expand_btt_xpaths:
            return None

        for xpath in expand_btt_xpaths:
            try:
                expand = WebDriverWait(self.driver, 5).until(
                    EC.presence_of_element_located((By.XPATH, xpath)))
                self.driver.execute_script(
                    "arguments[0].scrollIntoView();", expand)
                time.sleep(0.5)
                self.driver.execute_script("arguments[0].click();", expand)
                time.sleep(1)
                self.logger.log("Expanded description", level='INFO', site=self.site_name)
                break
            except NoSuchElementException as nse_error:
                self.logger.log("No expand description button",
                                level='ERROR',
                                site=self.site_name,
                                exception=nse_error)
                continue
            except TimeoutException as timeout_error:
                self.logger.log("Expand description button timed out",
                                level='ERROR',
                                site=self.site_name,
                                exception=timeout_error)
                continue
        return None

    def expand_tags_button(self):
        """
        Interact with expand tags button.
        """
        expand_btt_xpaths = self.xpaths.get("expand_tags_button", [])
        if expand_btt_xpaths == [""]:
            self.logger.log("No defined eexpand tags button xpaths",
                            level='ERROR', 
                            site=self.site_name)
            return None
        if not expand_btt_xpaths:
            return None

        for xpath in expand_btt_xpaths:
            try:
                expand_tags = WebDriverWait(self.driver, 5).until(
                    EC.presence_of_element_located((By.XPATH, xpath)))
                self.driver.execute_script(
                    "arguments[0].scrollIntoView();", expand_tags)
                time.sleep(0.5)
                self.driver.execute_script(
                    "arguments[0].click();", expand_tags)
                time.sleep(1)
                self.logger.log("Expanded tags", level='INFO', site=self.site_name)
                break
            except NoSuchElementException as nse_error:
                self.logger.log("No expand tags button",
                                level='ERROR',
                                site=self.site_name,
                                exception=nse_error)
                continue
            except TimeoutException as timeout_error:
                self.logger.log("Expand tags button timed out",
                                level='ERROR',
                                site=self.site_name,
                                exception=timeout_error)
                continue
        return None

    def ad_button(self):
        """
        Interact with ad button.
        """
        ad_btt_xpaths = self.xpaths.get("ad_button", [])
        if ad_btt_xpaths == [""]:
            self.logger.log("No defined ad button xpaths",
                            level='ERROR', 
                            site=self.site_name)
            return None
        if not ad_btt_xpaths:
            return None
        while True:
            for xpath in ad_btt_xpaths:
                try:
                    click_ad = WebDriverWait(self.driver, 1).until(
                        EC.presence_of_element_located((By.XPATH, xpath)))
                    self.driver.execute_script(
                        "arguments[0].scrollIntoView();", click_ad)
                    time.sleep(0.5)
                    self.driver.execute_script(
                        "arguments[0].click();", click_ad)
                    self.logger.log("Ad button clicked", level='INFO', site=self.site_name)
                    continue
                except NoSuchElementException as nse_error:
                    self.logger.log("No ad button",
                                level='ERROR',
                                site=self.site_name,
                                exception=nse_error)
                    continue
                except TimeoutException as timeout_error:
                    continue
