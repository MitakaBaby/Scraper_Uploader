import requests

class Taxonomies():
    def get_or_create_tag(self, tags_endpoint: str, tag_name: str, username: str, password: str) -> int:
        """
        Fetches an existing tag or creates a new one if it doesn't exist.

        This method sends a GET request to search for the tag by its name. If the tag is found, its ID is returned.
        If the tag is not found, a POST request is made to create the tag, and the new tag's ID is returned.

        Args:
            tags_endpoint (str): The URL of the API endpoint for fetching or creating tags.
            tag_name (str): The name of the tag to search for or create.
            username (str): The username for authentication.
            password (str): The password for authentication.

        Returns:
            int: The ID of the tag.
        """
        params = {'search': tag_name}
        response = requests.get(tags_endpoint, params=params, auth=(username, password))
        response.raise_for_status()
        data = response.json()

        if data:
            tag_id = data[0]['id']
            return tag_id

        payload = {'name': tag_name, 'slug': tag_name.lower().replace(' ', '-')}
        response = requests.post(tags_endpoint, json=payload, auth=(username, password))
        response.raise_for_status()
        data = response.json()
        tag_id = data['id']
        return tag_id 

    def get_category_number(self, site: str, category: str):
        """
        Retrieves the category number for a given site and category.

        This method uses predefined mappings to return a category number based on the provided site and category.
        If the site or category is not found in the mappings, it returns '0'.

        Args:
            site (str): The site for which the category number is requested.
            category (str): The category whose number is to be fetched.

        Returns:
            str: The category number as a string.
        """
        category_mappings = {
            'site1': {
                'News': '3',
                'Updates': '4',
                'History': '5',
                '': '',
                '': '',
                '': ''
            },
        }

        if site in category_mappings and category in category_mappings[site]:
            return category_mappings[site][category]
        else:
            return '0'
