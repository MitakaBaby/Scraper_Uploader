import requests
from functools import partial


from common import Credentials, WpEndpoints, Utils, Jsons, CustomLogger, Paths
from wp_upload.filters import Filters
from wp_upload.content import WpPostContent
from wp_upload.taxonomies import Taxonomies

utils = Utils()
jsons = Jsons()
filters = Filters()
wppostcontent = WpPostContent()
taxonomies = Taxonomies()
logger = CustomLogger()


def is_uploaded(title: str, site: str, uploaded_data: list) -> bool:
    """
    Checks if a record with the given title and site has already been uploaded.

    Args:
        title (str): The title of the record to check.
        site (str): The site where the record might have been uploaded.
        uploaded_data (list): A list of records that have already been uploaded.

    Returns:
        bool: True if the record has been uploaded, False otherwise.
    """
    for record in uploaded_data:
        if record.get('Title') == title and record.get(site) == True:
            return True
    return False

def process_uploading(site: str, record, uploaded_data, uploaded_posts):
    """
    Processes and uploads a record if it has not been uploaded yet.

    This function retrieves credentials and endpoint information, prepares data for upload including tags and categories,
    and uploads the data to the specified site. It also handles image uploads and updates the list of uploaded posts.

    Args:
        site (str): The site to upload data to.
        record (dict): The record containing data to be uploaded.
        uploaded_data (list): The list of already uploaded records.
        uploaded_posts (list): The list to update with newly uploaded post information.
    """
    title = record.get('Title')
    site_name = record.get('Site')
    if not is_uploaded(title, site_name, uploaded_data):
        creds = Credentials().credentials(site)
        if creds:
            username = creds['username']
            password = creds['password']
            link = creds['url']
            site_endpoints = WpEndpoints().endpoints(link)
            if site_endpoints:
                categories_endpoint = site_endpoints['categories']
                models = record.get('Models')
                if models:
                    tag_names = [site_name] + models.split(', ')
                    tag_ids = [taxonomies.get_or_create_tag(site_endpoints['tags'], tag_name, username, password) for tag_name in tag_names if tag_name]
                else:
                    tag_names = [site_name]
                    tag_ids = [taxonomies.get_or_create_tag(site_endpoints['tags'], tag_name, username, password) for tag_name in tag_names if tag_name]
                category = "New videos"
                category_number = taxonomies.get_category_number(site_name, category)
                html_title = wppostcontent.title_html(title)
                description = record.get('Description')
                html_description = wppostcontent.description_html(description, models)
                video = record.get('Video to embed')
                html_video = wppostcontent.video_html(video)
                promo_link = record.get("Link for promo")
                html_link = wppostcontent.promo_link_html(promo_link)
                html_rows = html_title + html_description + html_video + html_link
                path_img = record.get('Path image')
                image_data = wppostcontent.upload_image(username, password, site_endpoints['images'], path_img, title)
                if image_data:
                    data = {
                        'title': title,
                        'content': html_rows,
                        'tags': tag_ids,
                        'status': 'publish',
                        'categories': 55,  # 'categories': category_number,
                        'featured_media': image_data['id'],
                    }
                    response = requests.post(site_endpoints['posts'], json=data, auth=(username, password))
                    if response.status_code == 201:
                        post_data = response.json()
                        requests.post(
                            f"{site_endpoints['images']}/{image_data['id']}",
                            auth=(username, password),
                            json={'post': post_data['id']}
                        )
                        post_url = post_data['guid']['rendered']
                        new_record = {'Site': site_name, 'Title': title, 'Models': models, 'Url from site': post_url}
                        uploaded_posts.append(new_record)
                        logger.log(
                            f"Successfully uploaded post: {title}",
                            level='INFO',
                            site=None
                        )
                    else:
                        logger.log(f"Error uploading post '{title}'",
                            level='DFCRITICAL',
                            site=None)
                else:
                    logger.log(f"Couldn't upload picture for post: '{title}'",
                        level='DFCRITICAL',
                        site=None)
            else:
                logger.log(f"No endpoints found for site '{site_name}'",
                    level='DFCRITICAL',
                    site=None)
        else:
            logger.log(f"No credentials found for site '{site_name}'",
                level='DFCRITICAL',
                site=None)

uploaded_posts = []

def upload():
    """
    Handles the process of uploading records to the specified sites.

    This function filters the records, identifies which records have not been uploaded yet, and processes them for upload.
    It updates the list of uploaded posts and writes this data to storage.
    """
    sites = ["site1", "site2"]
    uploaded_posts = []
    jsons = Jsons()

    filters.apply_filters()
    filtered = jsons.lock_json(jsons.set_filtered(), partial(jsons.read_json, jsons.set_filtered())) or []
    uploaded_data = jsons.lock_json(jsons.set_uploaded(), partial(jsons.read_json, jsons.set_uploaded())) or []
    if filtered:
        if uploaded_data:
            not_uploaded_df = [record for record in filtered if record.get('Title') not in {rec.get('Title') for rec in uploaded_data}]
        else:
            not_uploaded_df = filtered.copy()

        if not_uploaded_df:
            logger.log(f"Records to be uploaded: {len(not_uploaded_df)}",
                        level='DFINFO',
                        site=None)
            for site in sites:
                logger.log(f"Started uploading to site: {site}",
                            level='DFINFO', 
                            site=None)
                for record in not_uploaded_df:
                    process_uploading(site, record, uploaded_data, uploaded_posts)
                logger.log(f"Finished uploading to site: {site}",
                           level='DFINFO',
                           site=None)
        else:
            logger.log("No new records to upload",
                        level='DFINFO',
                        site=None)

        jsons.lock_json(jsons.set_uploaded(), partial(jsons.write_json, uploaded_posts, jsons.set_uploaded()))
    else:
        logger.log("No filtered data available",
                    level='DFINFO',
                    site=None)
