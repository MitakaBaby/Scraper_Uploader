import re
from datetime import datetime, timedelta
from fuzzywuzzy import fuzz


from common import Utils, CustomLogger, Jsons

class Filters():

    def __init__(self) -> None:
        self.utils = Utils()
        self.jsons = Jsons()
        self.logger = CustomLogger()

    def ordered_filters(self, data: list) -> list:
        """
        Applies a sequence of filtering and cleaning operations to a list of data records.

        Args:
        - data (list): A list of dictionaries representing JSON data records.

        Returns:
        - data (list): The filtered and processed list of data records.

        Behavior:
        - This function processes the input data through a series of filtering and cleaning functions in a specific order:
        1. Removes records with empty titles.
        2. Removes records with missing images.
        3. Eliminates duplicate titles.
        4. Filters titles based on specific keywords.
        5. Replaces symbols in titles.
        6. Ensures models' names are included in titles.
        7. Validates models' names in the models list.
        8. Checks if titles match the models' names.
        9. Removes records older than three days.
        10. Sets promotional links based on site information.
        - The final processed data is returned after all filters are applied.
        """
        new_data = TitleFilters().clean_empty_titles(data)
        new_data = ImageFilters().clean_empty_image(new_data)
        new_data = TitleFilters().clean_duplicates(new_data)
        new_data = TitleFilters().containing_words(new_data)
        new_data = TitleFilters().replace_symb_title(new_data)
        new_data = TitleFilters().model_in_title(new_data)
        new_data = ModelsFilters().model_in_models(new_data)
        new_data = TitleFilters().title_equal_models(new_data)
        new_data = DateFilters().clean_older_than_3_days(new_data)
        new_data = PromoLinks().set_promo_links(new_data)

        return new_data

    def apply_filters(self):
        """
        Filters newly scrapped data by excluding already filtered records and applying a sequence of data cleaning operations.

        Behavior:
        - Reads daily scrapped data and previously filtered data from JSON files.
        - Identifies new data by comparing 'Link for video' values between the daily scrapped data and already filtered data.
        - Applies a series of ordered filtering and cleaning operations (using the `ordered_filters` method) to the new data.
        - Writes the filtered data back to the filtered JSON file.
        - Logs an error if the daily scrapped data or filtered data cannot be read.
        """
        daily_scrapped = self.jsons.read_json(self.jsons.set_daily_scrapped())
        filtered = self.jsons.read_json(self.jsons.set_filtered())

        if daily_scrapped is not None and filtered is not None:
            filtered_titles = {item.get('Link for video') for item in filtered}
            
            new_data = [item for item in daily_scrapped if item.get('Link for video') not in filtered_titles and item]
            
            if new_data:
                filtered_data = self.ordered_filters(new_data)
                self.jsons.write_json(filtered_data, self.jsons.set_filtered())
        else:
            self.logger.log(
                f"Failed to read data from one or both sources",
                level='ERROR',
                site="DataFrame") 

class DateFilters(Filters):

    def generate_uploaded_file_names(self) -> list[str]:
        """
        Generates a list of JSON file names representing uploaded data for the past five days.

        Returns:
        - data (list): A list of JSON file names in the format 'Uploaded+<Month Day, Year>.json'.

        Behavior:
        - Computes the current date and then generates file names for the previous five days.
        - Each file name corresponds to a specific date, formatted as 'Uploaded+<Month Day, Year>.json'.
        - Returns the list of these generated file names.
        """
        current_date = datetime.now()
        days=5
        file_names = []
        for day in range(1, days + 1):
            previous_date = current_date - timedelta(days=day)
            file_name = f"Uploaded+{previous_date.strftime('%b %d, %Y')}.json"
            file_names.append(file_name)

        return file_names

    def clean_older_than_3_days(self, data: list) -> list:
        """
        Remove records from the list of dictionaries that are older than 3 days or are similar to uploaded file titles.
        
        Args:
            - data (list): List of dictionaries to be filtered.

        Returns:
            - data (list): The filtered list of dictionaries.
        """
        uploaded_file_names = self.generate_uploaded_file_names()
        rows_to_keep = [True] * len(data)

        current_date = self.utils.get_current_date()
        current_date_obj = datetime.strptime(current_date, '%b %d, %Y')
        three_days_ago = current_date_obj - timedelta(days=3)

        removed_titles = []

        for i, record in enumerate(data):
            title_i = record.get('Title')
            date_i = record.get('Date', '')

            if date_i is None:
                date_i = ''

            if date_i:
                try:
                    date_i_obj = datetime.strptime(date_i, '%b %d, %Y')
                except ValueError:
                    date_i_obj = None
            else:
                date_i_obj = None

            if date_i_obj and date_i_obj < three_days_ago:
                rows_to_keep[i] = False
                removed_titles.append(title_i)
                continue

            for file_name in uploaded_file_names:
                file_path = self.jsons.uploaded_dir + f"\\{file_name}"
                uploaded_data = self.jsons.read_json(file_path)
                if uploaded_data:
                    for uploaded_record in uploaded_data:
                        title_j = uploaded_record.get('Title', '')
                        similarity_ratio = fuzz.ratio(title_i, title_j)
                        if similarity_ratio >= 90:
                            self.logger.log(
                                f"Similar titles found: '{title_i}' in current data and '{title_j}' in uploaded file '{file_path}'",
                                level='DFINFO',
                                site=None
                            )
                            rows_to_keep[i] = False
                            break

        filtered_data = [record for keep, record in zip(rows_to_keep, data) if keep]

        for removed_title in removed_titles:
            self.logger.log(
                f"Removed title: {removed_title} because it's older than 3 days",
                level='DFINFO',
                site=None
            )

        return filtered_data


class ImageFilters(Filters):

    def clean_empty_image(self, data: list) -> list:
        """
        Remove records from a list of dictionaries where the 'Path image' field is None.

        Args:
            - data (list): A list of dictionaries, where each dictionary represents a record.
                            The keys in each dictionary are strings, and values can vary in type.

        Returns:
            - data (list): The filtered list of dictionaries with 'Path image' field not None.
        """
        initial_count = len(data)
        filtered_data = [record for record in data if record.get('Path image') is not None]
        final_count = len(filtered_data)
        dropped_count = initial_count - final_count
        
        if dropped_count != 0:
            self.logger.log(
                f"Number of records without image dropped: {dropped_count}",
                level='DFINFO',
                site=None
            )
        
        return filtered_data


class ModelsFilters(Filters):

    def model_in_models(self, data: list) -> list:
        """
        Applying 'Models' name changes with specific rules(models_filter) to JSON data.

        Args:
            - data (list): List of dictionaries to be filtered.

        Returns:
            - data (list): The filtered list of dictionaries.
        """
        self.models_filter = self.jsons.load_models_filter()
        changes = []

        for site, models_dict in self.models_filter.items():
            for model_key, model_value in models_dict.items():
                for record in data:
                    if record.get('Site') == site:
                        original_models = record.get('Models', '')
                        if isinstance(original_models, str):
                            updated_models = []
                            for name in original_models.split(', '):
                                if name.strip() == model_key:
                                    updated_models.append(model_value)
                                else:
                                    updated_models.append(name)
                            updated_models = ', '.join(updated_models)

                            if original_models != updated_models:
                                record['Models'] = updated_models
                                changes.append((original_models, updated_models))

        original_models_after_replace = [record.get('Models', '') for record in data]
        for record in data:
            if isinstance(record.get('Models'), str):
                record['Models'] = record['Models'].replace('.', '').strip()

        for original, new in zip(original_models_after_replace, [record.get('Models', '') for record in data]):
            if original != new:
                changes.append((original, new))

        if changes:
            for original, new in changes:
                self.logger.log(
                    f"Model changed from '{original}' to '{new}'",
                    level='DFINFO',
                    site=None
                )

        return data

class PromoLinks(Filters):

    def set_promo_links(self, data: list) -> list:
        """
        Update 'Link for promo' based on 'Site' for each record in the data.
        
        Args:
            - data (list): List of dictionaries representing JSON data.
        
        Returns:
            - data (list): Modified list of dictionaries with updated 'Link for promo'.
        """
        for record in data:
            site = str(record.get('Site', '')).lower()
            promo_link = self.jsons.load_promo_link(site)
            if promo_link:
                record['Link for promo'] = str(promo_link)
            else:
                record['Link for promo'] = None

        return data

class TitleFilters(Filters):

    def clean_empty_titles(self, data: list) -> list:
        """
        Remove records from the list of dictionaries where the 'Title' field is None.

        Args:
            - data (list): List of dictionaries to be filtered.

        Returns:
            - data (list): The filtered list of dictionaries.
        """
        initial_count = len(data)
        filtered_data = [record for record in data if record.get('Title') is not None]
        final_count = len(filtered_data)
        dropped_count = initial_count - final_count
        
        if dropped_count != 0:
            self.logger.log(
                f"Number of records without title dropped: {dropped_count}",
                level='DFINFO',
                site=None
            )
        
        return filtered_data

    def replace_symb_title(self, data: list) -> list:
        """
        Replace certain symbols and clean up the 'Title' field in a list of dictionaries.

        Args:
            - data (list): List of dictionaries, each containing a 'Title' field.

        Returns:
            - data (list): The list of dictionaries with cleaned 'Title' fields.
        """
        for record in data:
            if 'Title' in record and record['Title'] is not None:
                title = record['Title']
                title = title.replace('-', '')\
                             .replace('|', '')\
                             .replace(':', '')\
                             .replace(',', '')\
                             .replace('.', '')\
                             .replace(')', '')\
                             .replace('(', '')\
                             .replace('#', '')\
                             .replace('’', "'")\
                             .replace('&', 'and')\
                             .replace('+', 'and')\
                             .replace('—', '')\
                             .replace('?', '')\
                             .replace('】', '')\
                             .replace('【', '')\
                             .replace('  ', ' ')
                title = re.sub(r'\s+', ' ', title).strip()
                record['Title'] = title
        
        return data

    def containing_words(self, data: list) -> list:
        """
        Remove records from the list of dictionaries where the 'Title' contains specific words.
        
        Args:
            - data (list): List of dictionaries to be filtered.

        Returns:
            - data (list): The filtered list of dictionaries.
        """
        filter_words = {'word1', 'word with word1', 'word2'}
        rows_to_keep = [True] * len(data)
        removed_titles = []

        for index, record in enumerate(data):
            title = record.get('Title', '')
            lowercase_title = title.lower()
            for word in filter_words:
                pattern = r'\b' + re.escape(word) + r'\b'
                if re.search(pattern, lowercase_title):
                    rows_to_keep[index] = False
                    removed_titles.append((title, word.capitalize()))
                    break

        filtered_data = [record for keep, record in zip(rows_to_keep, data) if keep]

        for removed_title, filter_word in removed_titles:
            self.logger.log(
                f"Removed title: {removed_title}, Filter word: {filter_word}",
                level='DFINFO',
                site=None
            )

        return filtered_data

    def clean_duplicates(self, data: list) -> list:
        """
        Remove records from the list of dictionaries where titles are similar based on fuzzy matching.
        
        Args:
            - data (list): List of dictionaries to be filtered.

        Returns:
            - data (list): The filtered list of dictionaries with duplicates removed.
        """
        rows_to_keep = [True] * len(data)
        site_rules = {
            ('site1', 'site2'): 'site1',
            ('site3', 'site4'): 'site3',
        }

        for i, record_i in enumerate(data):
            site_i = record_i.get('Site')
            title_i = record_i.get('Title', '')

            for j, record_j in enumerate(data[i + 1:], start=i + 1):
                site_j = record_j.get('Site')
                title_j = record_j.get('Title', '')
                similarity_ratio = fuzz.ratio(title_i, title_j)

                if similarity_ratio >= 99:
                    if (site_i, site_j) in site_rules or (site_j, site_i) in site_rules:
                        site_to_drop = site_rules.get((site_i, site_j), site_rules.get((site_j, site_i)))
                        if site_to_drop == site_i:
                            rows_to_keep[i] = False
                        elif site_to_drop == site_j:
                            rows_to_keep[j] = False
                    else:
                        rows_to_keep[j] = False
                        self.logger.log(
                            f"Dropped title from site {site_j} because it's similar to site {site_i} \n title {title_i}, they don't have defined rules",
                            level='DFINFO',
                            site=None
                        )
                        site_rules[(site_i, site_j)] = site_j

        filtered_data = [record for keep, record in zip(rows_to_keep, data) if keep]

        return filtered_data 

    def model_in_title(self, data: list) -> list:
        """
        Apply title changes based on a models filter dictionary and log any changes.

        Args:
            - data (list): List of dictionaries representing records.

        Returns:
            - data (list): Updated list of dictionaries with titles changed based on models_filter.
        """
        models_filter = self.jsons.load_models_filter()
        changes = []

        for site, models_dict in models_filter.items():
            for model_key, model_value in models_dict.items():
                for record in data:
                    if record.get('Site') == site:
                        title = record.get('Title', '')
                        if model_key in title:
                            original_title = title
                            new_title = title.replace(model_key, model_value)
                            if new_title != title:
                                record['Title'] = new_title
                                changes.append((original_title, new_title))

        if changes:
            for original, new in changes:
                self.logger.log(
                    f"Title changed from '{original}' to '{new}'",
                    level='DFINFO',
                    site=None
                )

        return data

    def title_equal_models(self, data: list) -> list:
        """
        Modify 'Title' by appending 'is at {site}' when 'Title' is equal to 'Models'.
        
        Args:
            - data (list): List of dictionaries representing JSON data.
        
        Returns:
            - data (list): Modified list of dictionaries.
        """
        for record in data:
            title = record.get('Title')
            models = record.get('Models')
            site = record.get('Site')

            if title and models and title == models:
                record['Title'] += f" is at {site}"

        return data
