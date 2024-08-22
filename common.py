import os
import re
import sys
import json
import time
import socket
import shutil


import pandas as pd
import tkinter as tk
from functools import partial
from tkinter import messagebox
from datetime import datetime
from urllib.parse import urlparse
from selenium import webdriver
from typing import Callable, Union, List, Dict, Optional
from filelock import FileLock, Timeout
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager


class Colors:
    """
    Provides ANSI escape sequences for text color, background color, and text style.
    """
    # Foreground colors
    FOREGROUND = {
        'BLACK': '\033[30m',
        'RED': '\033[31m',
        'GREEN': '\033[32m',
        'YELLOW': '\033[33m',
        'BLUE': '\033[34m',
        'MAGENTA': '\033[35m',
        'CYAN': '\033[36m',
        'WHITE': '\033[37m',
        'ORANGE': '\033[38;5;208m',
        'PURPLE': '\033[38;5;93m',
        'PINK': '\033[38;5;205m',
        'BROWN': '\033[38;5;94m',
        'LIGHT_GRAY': '\033[37;1m',
        'DARK_GRAY': '\033[30;1m',
        'LIGHT_RED': '\033[31;1m',
        'LIGHT_GREEN': '\033[32;1m',
        'LIGHT_YELLOW': '\033[33;1m',
        'LIGHT_BLUE': '\033[34;1m',
        'LIGHT_MAGENTA': '\033[35;1m',
        'LIGHT_CYAN': '\033[36;1m'
    }

    # Background colors
    BACKGROUND = {
        'BLACK_BACKGROUND': '\033[40m',
        'RED_BACKGROUND': '\033[41m',
        'GREEN_BACKGROUND': '\033[42m',
        'YELLOW_BACKGROUND': '\033[43m',
        'BLUE_BACKGROUND': '\033[44m',
        'MAGENTA_BACKGROUND': '\033[45m',
        'CYAN_BACKGROUND': '\033[46m',
        'WHITE_BACKGROUND': '\033[47m',
        'ORANGE_BACKGROUND': '\033[48;5;208m',
        'PURPLE_BACKGROUND': '\033[48;5;93m',
        'PINK_BACKGROUND': '\033[48;5;205m',
        'BROWN_BACKGROUND': '\033[48;5;94m',
        'LIGHT_GRAY_BACKGROUND': '\033[47;1m',
        'DARK_GRAY_BACKGROUND': '\033[40;1m',
        'LIGHT_RED_BACKGROUND': '\033[41;1m',
        'LIGHT_GREEN_BACKGROUND': '\033[42;1m',
        'LIGHT_YELLOW_BACKGROUND': '\033[43;1m',
        'LIGHT_BLUE_BACKGROUND': '\033[44;1m',
        'LIGHT_MAGENTA_BACKGROUND': '\033[45;1m',
        'LIGHT_CYAN_BACKGROUND': '\033[46;1m'
    }

    # Styles
    STYLE = {
        'BOLD': '\033[1m',
        'ITALIC': '\033[3m',
        'UNDERLINE': '\033[4m',
        'INVERSE': '\033[7m',
        'STRIKETHROUGH': '\033[9m',
        'BOLD_OFF': '\033[21m',
        'UNDERLINE_OFF': '\033[24m',
        'INVERSE_OFF': '\033[27m'
    }

    RESET = '\033[0m'

    @staticmethod
    def color(*args):
        """
        Returns ANSI escape sequences for applying styles and colors in the console.

        Args:
            *args (str): Styles and colors to apply. Can be 'BOLD', 'ITALIC', 'UNDERLINE', 'INVERSE',
                         or any of the predefined color names in the FOREGROUND and BACKGROUND dictionaries.

        Returns:
            str: ANSI escape sequences.
        """
        escape_sequence = ''

        style_count = sum(1 for arg in args if arg.upper() in Colors.STYLE)
        fg_color_count = sum(1 for arg in args if arg.upper() in Colors.FOREGROUND)
        bg_color_count = sum(1 for arg in args if arg.upper() in Colors.BACKGROUND)

        if style_count > 1:
            raise ValueError("Only one style can be specified.")
        if fg_color_count > 1:
            raise ValueError("Only one foreground color can be specified.")
        if bg_color_count > 1:
            raise ValueError("Only one background color can be specified.")

        for style in args:
            if style.upper() in Colors.STYLE:
                escape_sequence += Colors.STYLE[style.upper()]

        for fg_color in args:
            if fg_color.upper() in Colors.FOREGROUND:
                escape_sequence += Colors.FOREGROUND[fg_color.upper()]

        for bg_color in args:
            if bg_color.upper() in Colors.BACKGROUND:
                escape_sequence += Colors.BACKGROUND[bg_color.upper()]

        return escape_sequence


class CustomLogger:
    """
    Logs messages to the console and log files with colored output based on log levels.
    """
    LOG_LEVEL_COLORS = {
        #Scraper
        'DEBUG': Colors.color('MAGENTA'),
        'MISC': Colors.color('BLUE'),
        'INFO': Colors.color('GREEN'),
        'PATH': Colors.color("ORANGE"),
        'WARNING': Colors.color('YELLOW'),
        'ERROR': Colors.color('RED'),
        'CRITICAL': Colors.color('RED', 'UNDERLINE'),
        #Dataframes
        'DFINFO': Colors.color('GREEN','DARK_GRAY_BACKGROUND'),
        'DFWARNING': Colors.color('YELLOW','DARK_GRAY_BACKGROUND'),
        'DFERROR': Colors.color('RED','DARK_GRAY_BACKGROUND'),
        'DFCRITICAL': Colors.color('RED', 'UNDERLINE', 'DARK_GRAY_BACKGROUND')
    }

    def log(self, message: str, level: str, site: str | None , exception=None) -> None:
        """
        Logs a message to the console and to log files.

        Args:
            message (str): The message to be logged.
            level (str): The log level, which determines the color of the log message.
            site (str): The name of the site being logged.
            exception (Optional): The exception to be logged, if any.

        Returns:
            None
        """
        folder_name = Utils.get_current_date()

        console_output = f"{message}"
        color = self.LOG_LEVEL_COLORS.get(level, '')
        print(f"{color}{console_output}{Colors.RESET}")

        log_entry = f"{Utils.get_current_time()} [{level}]"
        if site:
            log_entry += f" [{site}]"
        log_entry += f" {message}"

        if exception:
            log_entry += "\n" + str(exception)
        folder_path = os.path.join(Paths().log_dir, folder_name)
        os.makedirs(folder_path, exist_ok=True)

        if level in ['INFO', 'PATH', 'MISC', 'DFINFO']:
            level = 'INFO'
        log_file = os.path.join(folder_path, f"{level.lower()}.log")

        with open(log_file, 'a', encoding='utf-8') as f:
            f.write(log_entry + '\n')

        main_log_file = os.path.join(folder_path, "main.log")
        with open(main_log_file, 'a', encoding='utf-8') as f:
            f.write(log_entry + '\n')


class Paths:
    """
    Manages file paths and directory creation.
    """
    def __init__(self):
        """
        Initializes Paths object.
        """
        self.home_dir = os.path.expanduser("~")
        self.filename = os.path.basename(__file__)
        self.script_dir = os.path.dirname(__file__)
        self.data_dir = os.path.join(self.home_dir, "Desktop", "Site", "Data")
        self.desktop_dir = os.path.join(self.home_dir, self.data_dir, "Data From Scrapers")
        self.image_dir = os.path.join(self.home_dir, self.data_dir, "Pictures", "Auto Downloaded")
        self.video_dir = os.path.join(self.home_dir, self.data_dir, "Videos", "Auto Downloaded Trailers")
        self.raw_data_dir = os.path.join(self.home_dir, self.data_dir, "Raw Data")
        self.filtered_dir = os.path.join(self.home_dir, self.data_dir, "Filtered Data")
        self.uploaded_dir = os.path.join(self.home_dir, self.data_dir, "Uploaded")
        self.social_accounts_dir = os.path.join(self.home_dir, self.data_dir, "Social Accounts Database")
        self.log_dir = os.path.join(self.home_dir, self.data_dir, "Logs")
        self.jsons = os.path.join(self.script_dir, 'jsons')
        self.date_utils = Utils()
        self.logger = CustomLogger()
        self.create_directories()

    def set_daily_scrapped(self) -> str:
        """
        Set the file path for the daily scrapped data and create a JSON file if it does not exist.

        Returns:
            str: File path for the daily scrapped data JSON file.
        """
        daily_scrapped = os.path.join(
            self.raw_data_dir, f"DailyScrapped+{self.date_utils.get_current_date()}.json")
        
        Jsons().lock_json(daily_scrapped, partial(Jsons().create_or_check_json, daily_scrapped))
        
        return daily_scrapped
    
    def set_site_scrapped(self, site_name: str) -> str:
        """
        Set the file path for the site scrapped data and create a JSON file if it does not exist.

        Returns:
            str: File path for the site scrapped data JSON file.
        """
        self.site_scrapped = os.path.join(self.desktop_dir, f"{site_name}.json")
        Jsons().lock_json(self.site_scrapped, partial(Jsons().create_or_check_json, self.site_scrapped))
        return self.site_scrapped
    
    def set_filtered(self) -> str:
        """
        Set the file path for the filtered data and create a JSON file if it does not exist.

        Returns:
            str: File path for the filtered data JSON file.
        """
        filtered = os.path.join(
            self.filtered_dir, f"Filtered Data+{self.date_utils.get_current_date()}.json")
        Jsons().lock_json(filtered, partial(Jsons().create_or_check_json, filtered))
        return filtered
    
    def set_uploaded(self) -> str:
        """
        Set the file path for the uploaded data and create a JSON file if it does not exist.

        Returns:
            str: File path for the uploaded data JSON file.
        """
        uploaded = os.path.join(
            self.uploaded_dir, f"Uploaded+{self.date_utils.get_current_date()}.json")
        Jsons().lock_json(uploaded, partial(Jsons().create_or_check_json, uploaded))
        return uploaded

    def create_video_path(self, site_name: str, counter_vid: int):
        """
        Create a path for a video file based on the site name and video counter.

        Parameters:
            site_name (str): Name of the site.
            counter_vid (int): Counter for the video.

        Returns:
            str: Path for the video file.
        """
        folder_path_video = os.path.join(self.video_dir, site_name)
        os.makedirs(folder_path_video, exist_ok=True)

        path_video = os.path.join(
            folder_path_video, f"{self.date_utils.get_current_datetime()}-{counter_vid}.mp4")

        return path_video

    def create_image_path(self, site_name: str, counter_img: int):
        """
        Create a path for a image file based on the site name and image counter.

        Parameters:
            site_name (str): Name of the site.
            counter_img (int): Counter for the image.

        Returns:
            str: Path for the image file.
        """

        folder_path_image = os.path.join(self.image_dir, site_name)
        os.makedirs(folder_path_image, exist_ok=True)

        path_image = os.path.join(
            folder_path_image, f"{self.date_utils.get_current_datetime()}-{counter_img}.jpg")

        return path_image

    def create_directories(self):
        """ 
        Create necessary directories if they don't exist. 
        """
        os.makedirs(self.data_dir, exist_ok=True)
        os.makedirs(self.desktop_dir, exist_ok=True)
        os.makedirs(self.image_dir, exist_ok=True)
        os.makedirs(self.video_dir, exist_ok=True)
        os.makedirs(self.raw_data_dir, exist_ok=True)
        os.makedirs(self.filtered_dir, exist_ok=True)
        os.makedirs(self.uploaded_dir, exist_ok=True)
        os.makedirs(self.log_dir, exist_ok=True)
        os.makedirs(self.jsons, exist_ok=True)

"""    def set_daily_scrapped(self):
        '''
        Set the file path for the daily scrapped data and create an Excel file if it does not exist.

        Returns:
            str: File path for the daily scrapped data Excel file.
        '''
        self.daily_scrapped = os.path.join(
            self.raw_data_dir, f"DailyScrapped+{self.date_utils.get_current_date()}.xlsx")

        if not os.path.exists(self.daily_scrapped):
            df = pd.DataFrame(columns=["Site", "Date", "Title", "Description", "Tags", "Models", "Video to embed",
                                       "Link for video", "Link for image", "Path image", "Path video"])
            df.to_excel(self.daily_scrapped, index=False)

        return self.daily_scrapped
    
    def set_site_scrapped(self, site_name):
        '''
        Set the file path for the site scrapped data and create an Excel file if it does not exist.

        Returns:
            str: File path for the site scrapped data Excel file.
        '''
        self.site_scrapped = os.path.join(
            self.desktop_dir, f"{site_name}.xlsx")
        if not os.path.exists(self.site_scrapped):
            df = pd.DataFrame(columns=["Site", "Date", "Title", "Description", "Tags", "Models", "Video to embed",
                                       "Link for video", "Link for image", "Path image", "Path video"])
            df.to_excel(self.site_scrapped, index=False)
        return self.site_scrapped
   

    def set_filtered(self):
        self.filtered = os.path.join(
            self.filtered_dir, f"Filtered Data+{self.date_utils.get_current_date()}.xlsx")
        if not os.path.exists(self.filtered):
            df = pd.DataFrame(columns=["Site", "Date", "Title", "Description", "Tags", "Models", "Video to embed",
                                       "Link for video", "Link for image", "Path image", "Path video", "Link for promo"])
            df.to_excel(self.filtered, index=False)
        return self.filtered
   
    def set_uploaded(self):
        self.uploaded = os.path.join(self.uploaded_dir, f"Uploaded+{self.date_utils.get_current_date()}.xlsx")
        required_columns = ["Site", "Title", "Models", "Url from site", "Twitter", "Reddit"]

        if os.path.exists(self.uploaded):
            existing_df = pd.read_excel(self.uploaded)
            for col in required_columns:
                if col not in existing_df.columns:
                    existing_df[col] = None
            existing_df.to_excel(self.uploaded, index=False)
        else:
            df = pd.DataFrame(columns=required_columns)
            df.to_excel(self.uploaded, index=False)

        return self.uploaded"""

'''
class DataFrames(Paths):
    """
    Manages the creation and manipulation of Pandas DataFrames for storing scraped data.
    Inherits from Paths to utilize file paths and directory management functionalities.
    """

    def save_dataframe_with_retry(self, data, output_path, site_name=None):
        """ 
        Save DataFrame to Excel file with retry mechanism in case of permission errors.

        Parameters:
            data (list or DataFrame): Data to be saved.
            output_path (str): Path to the output Excel file.
            site_name (str): Name of the site (optional).

        Returns:
            DataFrame: The DataFrame that was saved.
        """
        saved_successfully = False
        df = None
        if output_path == 'daily':
            output_path = self.set_daily_scrapped()
        elif output_path == 'site':
            output_path = self.set_site_scrapped(site_name)
        new_data = len(data)
        for attempt in range(5):
            try:
                df = pd.DataFrame(data, columns=["Site", "Date", "Title", "Description", "Tags", "Models", "Video to embed",
                                                 "Link for video", "Link for image", "Path image", "Path video"])
                existing_df = pd.read_excel(output_path)
                df = pd.concat([df, existing_df], ignore_index=True)
                if new_data:
                    df.to_excel(output_path, index=False)
                    saved_successfully = True
                break
            except PermissionError as ps_error:
                self.logger.log(
                    f"Attempt {attempt + 1}: A permission error occurred while saving the file {output_path}",
                    level='DFERROR',
                    site=None,
                    exception=ps_error)

                time.sleep(5)

        if not saved_successfully:
            if new_data:
                self.logger.log(
                    f"Exceeded the maximum number of retry attempts. The file {output_path} may be opened",
                    level='DFERROR',
                    site=None
                )

                success = self.manual_retry_prompt(
                    output_path, max_retries=3, data=data)
                if success:
                    self.logger.log(
                        "Manual attempt successful",
                        level='DFINFO',
                        site=None
                    )
                else:
                    self.logger.log(
                        "Manual attempt failed",
                        level='DFERROR',
                        site=None
                    )

        return df

    def manual_retry_prompt(self, output_path, max_retries, data):
        """ Display retry prompt """
        root = tk.Tk()
        root.withdraw()
        retry_count = 0
        while retry_count < max_retries:
            retry = messagebox.askretrycancel(
                "Retry", f"Exceeded the maximum number of retry attempts to save file {output_path}. Retry? ({retry_count + 1}/{max_retries})")
            if retry:
                retry_count += 1
                try:
                    df = pd.DataFrame(data, columns=["Site", "Date", "Title", "Description", "Tags", "Models", "Video to embed",
                                                     "Link for video", "Link for image", "Path image", "Path video"])
                    existing_df = pd.read_excel(output_path)
                    df = pd.concat([df, existing_df], ignore_index=True)
                    if data:
                        df.to_excel(output_path, index=False)
                        root.destroy()
                        return True
                except Exception as e:
                    self.logger.log(
                        f"Manual attempt failed. Error is {e}",
                        level='DFERROR',
                        site=None
                    )
            else:
                root.destroy()
                return False
        root.destroy()
        return False

    def read_dailyscrapped(self):
        self.set_daily_scrapped()
        try:
            df = pd.read_excel(self.daily_scrapped)
            return df
        except Exception as e:
            self.logger.log(
                f"An error occurred while reading the file",
                level='DFERROR',
                site=None,
                exception=e)
            return None
    
    def read_filtered(self):
        self.set_filtered()
        try:
            df = pd.read_excel(self.filtered)
            return df
        except Exception as e:
            self.logger.log(
                f"An error occurred while reading the file",
                level='DFERROR',
                site=None,
                exception=e)
            return None
        
    def read_uploaded(self, file_name = None):
        self.set_uploaded()
        if file_name:
            try:
                df = pd.read_excel(os.path.join(self.uploaded_dir, file_name))
                return df
            except Exception as e:
                self.logger.log(
                    f"An error occurred while reading the file",
                    level='DFERROR',
                    site=None,
                    exception=e)
                return None
        else:
            try:
                df = pd.read_excel(self.uploaded)
                return df
            except Exception as e:
                self.logger.log(
                    f"An error occurred while reading the file",
                    level='ERROR',
                    site="DataFrame",
                    exception=e)
                return None

    def save_filtered(self, combined_df):
        try:
            filtered_path = self.set_filtered()
            existing_df = pd.read_excel(filtered_path)
            updated_df = pd.concat([combined_df, existing_df], ignore_index=True)
            updated_df = updated_df.drop_duplicates()
            updated_df.to_excel(filtered_path, index=False)
            self.logger.log(
                f"Data saved to {filtered_path} successfully",
                level='DFINFO',
                site=None
                )
        except Exception as e:
            self.logger.log(
                "An error occurred while saving the file",
                level='DFERROR',
                site=None,
                exception=e)

    def save_uploaded(self, df):
        """
        Save the provided DataFrame to the uploaded file path.
        """
        saved_successfully = False
        output_path = self.set_uploaded()
        for attempt in range(5):
            try:
                existing_df = pd.read_excel(output_path)
                updated_df = pd.concat([df, existing_df], ignore_index=True)
                updated_df.to_excel(output_path, index=False)
                saved_successfully = True
                break
            except PermissionError as ps_error:
                self.logger.log(
                    f"Attempt {attempt + 1}: A permission error occurred while saving the file {output_path}",
                    level='DFRROR',
                    site=None,
                    exception=ps_error)
                time.sleep(5)

        if not saved_successfully:
            self.logger.log(
                f"Exceeded the maximum number of retry attempts. The file {output_path} may be opened",
                level='DFERROR',
                site=None
            )

            success = self.manual_retry_prompt(output_path, max_retries=3, data=updated_df)
            if success:
                self.logger.log(
                    "Manual attempt successful",
                    level='DFINFO',
                    site=None
                )
            else:
                self.logger.log(
                    "Manual attempt failed",
                    level='DFERROR',
                    site=None
                )
'''

class Jsons(Paths):


    def lock_json(self, file_path: str, operation: Callable[[], None]) -> None:
        """
        Perform a file operation with a file lock and retry mechanism.

        Args:
            file_path (str): Path to the file.
            operation (Callable[[], None]): Function that performs the operation.

        Returns:
            None
        """
        lock_path = file_path + ".lock"
        lock = FileLock(lock_path, timeout=10)

        attempt = 0
        while attempt < 5:
            try:
                with lock:
                    return operation()
            except Timeout:
                attempt += 1
                print(f"Attempt {attempt} failed. Retrying in 5 seconds...")
                time.sleep(5)

        print(f"Failed to acquire lock after 5 attempts.")

    def create_or_check_json(self, file_path) -> None:
        """
        Creates a new JSON file with an empty list if the file does not exist.

        Args:
            file_path (str): Path to the JSON file to be created or checked.

        Returns:
            None
        """
        try:
            if not os.path.exists(file_path):
                with open(file_path, 'w', encoding='utf-8') as file:
                    json.dump([], file, indent=4, ensure_ascii=False)
        except Exception as e:
            self.logger.log(
                f"Error while creating or checking JSON file",
                level='DFRROR',
                site=None,
                exception=e)

    def read_json(self, file_path: str):
        """
        Read the JSON data from the file.

        Args:
            file_path (str): Path to the file.

        Returns:
            list: The JSON data.
        """
        with open(file_path, 'r', encoding='utf-8') as file:
            data = json.load(file)
            return data

    def write_json(self, new_data: Union[Dict, List[Dict]], file_path: str, site_name: Optional[str] = None) -> None:
        """
        Write new data to the JSON file. If records with the same titles exist, update them.

        Args:
            file_path (str): Path to the file.
            new_data (Union[Dict, List[Dict]]): List of new data records to insert or update in the JSON file.
            site_name (Optional[str]): Name of the site (optional).

        Returns:
            None
        """
        if file_path == 'daily':
            file_path = self.set_daily_scrapped()
        elif file_path == 'site':
            if site_name is None:
                raise ValueError("Site name must be provided")
            file_path = self.set_site_scrapped(site_name)
        else:
            file_path = file_path
            
        data = self.read_json(file_path)

        if isinstance(new_data, dict):
            new_data = [new_data]

        for new_record in new_data:
            updated = False
            for i, record in enumerate(data):
                if record.get('Link for video'):
                    if record['Link for video'] == new_record.get('Link for video'):
                        data[i] = new_record
                        updated = True
                        break
                elif record.get('Title') == new_record.get('Title'):
                    data[i] = new_record
                    updated = True
                    break

            if not updated:
                data.insert(0, new_record)

        with open(file_path, 'w', encoding='utf-8') as file:
            json.dump(data, file, ensure_ascii=False, indent=4)

    @staticmethod
    def load_configs(site):
        """
        Load xpaths from a JSON file.

        Args:
            site (str): The name of the site.

        Returns:
            dict: A dictionary of xpaths for the given site.
        """
        with open(os.path.join(Paths().jsons, 'sites_config.json'),
                  'r', encoding='utf-8') as json_file:
            xpaths = json.load(json_file)

            xpaths_lower = {key.lower(): value for key, value in xpaths.items()}

            return xpaths_lower.get(site.lower(), {})
        
    @staticmethod
    def load_models_filter():
        """
        Load models filter.

        Returns:
        list: A list of models filter.
        """
        with open(os.path.join(Paths().jsons, 'models_filter.json'),
                  'r', encoding='utf-8') as json_file:
            models_filter = json.load(json_file)
            
            return models_filter
        
    @staticmethod
    def load_ps_link_site():
        """
        Load stars links in site filter.

        Returns:
        list: A list of stars links in site filter.
        """
        with open(os.path.join(Paths().jsons, 'ps_links_site.json'),
                  'r', encoding='utf-8') as json_file:
            ps_link_site = json.load(json_file)
            
            return ps_link_site

    @staticmethod
    def load_promo_link(site):
        """
        Get promo links.

        Returns:
        list: A list of promo links.
        """
        file_path = os.path.join(Paths().jsons, 'promo_links.json')

        with open(file_path, 'r', encoding='utf-8') as json_file:
            links = json.load(json_file)
            links_lower = {key.lower(): value for key, value in links.items()}

            site_lower = site.lower()

            return links_lower.get(site_lower) if site_lower in links_lower else None


class Credentials:


    def credentials(self, site: str) -> dict[str, str] | None:
        """
        Retrieves the credentials for a given site from a predefined dictionary.
        
        Args:
            site (str): The name of the site for which credentials are requested.

        Returns:
            dict or None: A dictionary containing 'username', 'password', and 'url' if the site exists in the credentials dictionary,
                        otherwise logs a critical error and returns None.
        """
        sites_credentials = {
            'site1': {
                'username': 'username',
                'password': 'password',
                'url': 'site1.com'
            },
        }
        if site in sites_credentials:
            return sites_credentials[site]
        else:
            CustomLogger().log(
                f"{site} is missing credentials",
                level='CRITICAL',
                site=None
                )
            return None


class WpEndpoints:

    
    def endpoints(self, link:str) -> dict:
        """
        Constructs and returns a dictionary of WordPress REST API endpoints for a given site.

        Args:
            link (str): The base URL of the WordPress site.

        Returns:
            dict: A dictionary where keys are endpoint types ('posts', 'images', 'categories', 'tags') and
                  values are the corresponding full URLs to those endpoints.
        """
        endpoint = {
            'posts': f'https://{link}/wp-json/wp/v2/posts',
            'images': f'https://{link}/wp-json/wp/v2/media',
            'categories': f"https://{link}/wp-json/wp/v2/categories",
            'tags': f"https://{link}/wp-json/wp/v2/tags"
        }
        
        return endpoint


class Popups:


    def __init__(self) -> None:
        pass

    def space_error_popup(self, retry_callback):
        """
        Show a popup message indicating that the disk space is insufficient with a Retry button.

        Args:
            retry_callback (function): The function to call when the retry button is clicked.
        """
        def on_retry():
            root.destroy()
            retry_callback()

        root = tk.Tk()
        root.withdraw()
        root.deiconify()
        root.title("Disk Space Error")
        
        label = tk.Label(root, text="No space left on device. Free up some space and click Retry.")
        label.pack(padx=20, pady=10)
        
        retry_button = tk.Button(root, text="Retry", command=on_retry)
        retry_button.pack(pady=10)
        
        root.mainloop()


class Utils:
    """
    Utility class for common functions.
    """
    @staticmethod
    def setup_chrome_driver(headless=True):
        """
        Setup chrome driver.

        Returns:
        driver.
        """
        chrome_options = webdriver.ChromeOptions()
        chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
        chrome_options.add_experimental_option('excludeSwitches', ['enable-logging'])
        chrome_options.add_argument("--start-maximized")
        chrome_options.add_argument("--disable-blink-features=AutomationControlled")
        chrome_options.add_argument('--log-level=3')
        chrome_options.add_argument('--mute-audio')
        chrome_options.add_argument('--disable-logging')
        chrome_options.add_argument('--disable-dev-shm-usage')
        chrome_options.add_argument('--disable-remote-debugging')
        chrome_options.add_argument('--disable-infobars')
        chrome_options.add_argument("--disable-search-engine-choice-screen")

        if headless:
            chrome_options.add_argument("--headless")

        #service = Service(ChromeDriverManager().install())
        chrome_install = ChromeDriverManager().install()
        folder = os.path.dirname(chrome_install)
        chromedriver_path = os.path.join(folder, "chromedriver.exe")
        service = Service(chromedriver_path)

        driver = webdriver.Chrome(service=service, options=chrome_options)

        return driver

    @staticmethod
    def get_current_date():
        """
        Returns current date in format: Month day, year (Jan 08, 2020)
        """
        return datetime.now().strftime("%b %d, %Y")

    @staticmethod
    def get_day_of_week():
        """
        Returns the current day of the week (0 for Monday, 6 for Sunday).
        """
        return datetime.now().weekday()

    @staticmethod
    def get_current_datetime():
        """
        Returns current date + time in format: 
        """
        return datetime.now().strftime("%Y-%m-%d %H-%M-%S")

    @staticmethod
    def get_current_time():
        """
        Returns current time in format:  hour : minutes : second (21:04:32)
        """
        return datetime.now().strftime("%H:%M:%S")

    @staticmethod
    def start_time():
        """
        Returns the current time as a starting point for measuring elapsed time.
        """
        return time.time()

    @staticmethod
    def end_time():
        """
        Returns the current time as an ending point for measuring elapsed time.
        """
        return time.time()

    @staticmethod
    def log_start_time(site):
        """
        Logs the start time of an execution.

        Args:
            site (str): The name of the site or process being logged.

        Returns:
            float: The start time in seconds since the epoch.
        """
        start_time = Utils.start_time()
        CustomLogger().log(
            f"{site} started executing at {Utils.get_current_time()}",
            level='MISC',
            site=site)
        return start_time

    @staticmethod
    def log_elapsed_time(start_time, site):
        """
        Logs the elapsed time since the start time.

        Args:
            start_time (float): The start time in seconds since the epoch.
            site (str): The name of the site or process being logged.

        Returns:
            None
        """
        end_time = Utils.end_time()
        elapsed_time = end_time - start_time
        CustomLogger().log(
            f"Elapsed time: {elapsed_time:.2f} seconds\n{'_' * 100}",
            level='MISC',
            site=site)

    @staticmethod
    def extract_site_name(url):
        """
        Extracts the site name from the given URL.

        Args:
            url (str): The URL from which to extract the site name.

        Returns:
            str: The extracted site name.
        """
        parsed_url = urlparse(url)
        match = re.match(
            r"^(?:https?://)?(?:www\.)?(?:.*?\.)?(?P<site_name>.+?)\.", parsed_url.netloc)
        site_name = match.group("site_name").replace(
            "-", "").replace("tour.", "").title() if match else ""

        return site_name

    @staticmethod
    def load_site_config(site):
        """
        Load the site configuration and return the site URL and name.

        Args:
            site (str): The name of the site.

        Returns:
            tuple: A tuple containing the site URL and name.
        """
        url_site = Jsons.load_configs(site).get("site")
        site_name = Utils.extract_site_name(url_site)
        return url_site, site_name

#    @staticmethod
#    def get_existing_data(site_name):
#        """
#        Retrieves existing data (links and titles) from a saved DataFrame for a given site.
#
#        Args:
#            site_name (str): The name of the site.
#
#        Returns:
#            tuple: A tuple containing lists of existing links and titles.
#                - If no data is found, returns empty lists.
#        """
#        data = []
#        result = DataFrames().save_dataframe_with_retry(data, "site", site_name)
#        if result is None:
#            return [], []
#        else:
#            link_from_excel = result['Link for video'].tolist()
#            title_from_excel = result['Title'].tolist()
#            return link_from_excel, title_from_excel
        
    @staticmethod
    def get_existing_data(site_name):
        
        data = Jsons().read_json(Paths().set_site_scrapped(site_name))
        link_from_json = []
        title_from_json = []

        for record in data:
            link_from_json.append(record.get('Link for video', ''))
            title_from_json.append(record.get('Title', ''))

        return link_from_json, title_from_json

#    @staticmethod
#    def save_scraped_data(data, site_name):
#        """
#        Saves scraped data to Excel files.
#
#        Args:
#            data (list or DataFrame): Data to be saved.
#            site_name (str): Name of the site for which the data is being saved.
#
#        Returns:
#            None
#        """
#        DataFrames().save_dataframe_with_retry(data, "daily")
#        DataFrames().save_dataframe_with_retry(data, "site", site_name)

    @staticmethod
    def save_scraped_data(data, site_name: str):
        """
        Saves scraped data to JSON files.

        Args:
            data (list): Data to be saved.
            site_name (str): Name of the site for which the data is being saved.

        Returns:
            None
        """
        Jsons().lock_json("daily" , partial(Jsons().write_json, data, "daily"))
        Jsons().lock_json("site" , partial(Jsons().write_json, data, "site", site_name=site_name))

    @staticmethod
    def print_progress(last_percentage, bytes_downloaded, total_size):
        """
        Print the progress of bytes downloaded.

        Args:
            last_percentage (float): Last percentage value printed.
            bytes_downloaded (int): Number of bytes downloaded.
            total_size (int): Total size of the file.
        """
        percentage = (bytes_downloaded / total_size) * 100
        if percentage != last_percentage:
            progress_bar_length = 50
            filled_length = int(progress_bar_length * bytes_downloaded / total_size)
            progress_bar = '█' * filled_length + '-' * (progress_bar_length - filled_length)

            progress_color = '\033[33m' if percentage < 100 else '\033[32m'
            blue_color = '\033[34m'
            end_color = '\033[0m'

            downloaded_mb = bytes_downloaded / (1024 * 1024)
            total_mb = total_size / (1024 * 1024)

            message = f"\r{blue_color}Progress:{end_color} {progress_color}|{progress_bar}| {percentage:.2f}% " \
                      f"{blue_color}{downloaded_mb:.2f}/{total_mb:.2f} MB{end_color}"
            
            sys.stdout.write(message)
            sys.stdout.flush()

            if percentage == 100:
                print()

        return percentage

    @staticmethod
    def download_media(response, path):
        """
        Download the media content from the response and save it to the specified path.

        Args:
            response (requests.Response): Response object containing media content.
            path (str): Path to save the media file.

        Returns:
            bool: True if download and save are successful, False otherwise.
        """
        try:
            total_size = int(response.headers.get('content-length', 0))
            bytes_downloaded = 0
            block_size = 1024
            last_percentage = -1

            with open(path, 'wb') as file:
                for data in response.iter_content(chunk_size=block_size):
                    file.write(data)
                    bytes_downloaded += len(data)
                    last_percentage = Utils().print_progress(last_percentage, bytes_downloaded, total_size)

            return True
        except Exception as e:
            CustomLogger().log(
                f"Error occurred during download: {e}",
                level='ERROR',
                site=None,
                exception=e
            )
            return False

    @staticmethod
    def check_network_connection():
        """
        Checks network connection by testing connectivity to DNS (port 53) and HTTP (port 80).

        Returns:
        - bool: True if both DNS and HTTP ports are reachable, False otherwise.
        """
        def check_port(host, port, timeout=5):
            try:
                socket.create_connection((host, port), timeout=timeout)
                return True
            except OSError:
                return False

        dns_check = check_port("8.8.8.8", 53)
        http_check = check_port("www.google.com", 80)
        return dns_check and http_check

    @staticmethod
    def check_disk_space(path: str, required_space: int) -> bool:
        """
        Check if there is enough disk space available at the specified path.
        
        Args:
            path (str): The path to check the disk space for.
            required_space (int): The amount of space required in bytes.
        
        Returns:
            bool: True if there is enough space, False otherwise.
        """
        total, used, free = shutil.disk_usage(path)
        return free > required_space
