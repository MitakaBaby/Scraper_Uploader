import os
import json
import requests
from requests.exceptions import ConnectionError, Timeout


from common import CustomLogger, Jsons

class WpPostContent():

    def __init__(self) -> None:
        self.jsons = Jsons()
        self.logger = CustomLogger()

    def description_html(self, description: str, models_str: str) -> str:
        """
        Generates HTML for a given description by splitting it into paragraphs, ensuring each paragraph is no longer than 500 characters. It then replaces model names in the description with corresponding hyperlinks if they exist in a predefined dictionary of model links.

        Args:
        - description (str): The text description to be converted into HTML.
        - models_str (str): A comma-separated string of model names to be hyperlinked.

        Returns:
        - str: The HTML-formatted description with paragraphs and optional hyperlinks.
        """
        if isinstance(description, str) and description and description != "-":
            paragraphs = []
            while len(description) > 0:
                if len(description) <= 500:
                    paragraphs.append(description)
                    description = ""
                else:
                    paragraph = description[:500]
                    last_period = max(paragraph.rfind("."), paragraph.rfind("!"), paragraph.rfind("?"))
                    if last_period == -1:
                        paragraphs.append(paragraph)
                        description = description[500:]
                    else:
                        paragraphs.append(description[:last_period+1])
                        description = description[last_period+1:]
            html_description = ""
            for paragraph in paragraphs:
                html_description += "<!-- wp:paragraph -->\n<p>" + paragraph + "</p>\n<!-- /wp:paragraph -->\n"
            
            replacement_dict = {}
            ps_link_site = self.jsons.load_ps_link_site()
            if models_str:
                models = [model.strip() for model in models_str.split(',')]

                for model in models:
                    matching_models = [key for key in ps_link_site if key.lower() == model.lower()]

                    if matching_models:
                        replacement_model = matching_models[0]
                        replacement_text = f'<a href="/index.php/{ps_link_site[replacement_model]}" data-type="link" data-id="/index.php/{ps_link_site[replacement_model]}">{replacement_model}</a>'
                        replacement_dict[model] = replacement_text

                for key, value in replacement_dict.items():
                    html_description = html_description.replace(key, value)

        else:
            html_description = ""

        return html_description
    
    def upload_image(self, username: str, password: str, images_endpoint: str, image_path: str, title: str):
        """
        Attempts to upload an image to a specified endpoint with basic authentication, retrying the operation if it fails.

        Args:
        - username (str): Username for authentication.
        - password (str): Password for authentication.
        - images_endpoint (str): The URL endpoint to which the image should be uploaded.
        - image_path (str): The file path of the image to upload.
        - title (str): The alt text/title for the image.

        Returns:
        - dict: A dictionary containing the image ID and URL if the upload is successful.
        - None: If the image could not be uploaded.

        Behavior:
        - Opens the image file and attempts to upload it to the provided endpoint.
        - If the upload fails, the function retries up to `retry_count` times.
        - Logs success, failure, and error messages appropriately.
        - Returns the image ID and URL if successful, or None if all attempts fail.
        """
        retry_count=3
        try:
            with open(image_path, 'rb') as f:
                retries = 0
                while retries < retry_count:
                    try:
                        response = requests.post(
                            images_endpoint, 
                            auth=(username, password), 
                            files={'file': (os.path.basename(image_path), f, 'image/jpeg')},
                            data={'alt_text': title}
                        )
                        if response.status_code == 201:
                            image_id = response.json()['id']
                            image_url = response.json()['guid']['rendered']
                            self.logger.log(
                                f"Successfully uploaded image for: {title}",
                                level='INFO',
                                site=None
                            )
                            return {'id': image_id, 'url': image_url}
                        else:
                            self.logger.log(
                                f"Failed to upload image at path: {image_path}",
                                level='ERROR',
                                site=None
                            )
                            self.logger.log(
                                f"Response status code: {response.status_code}",
                                level='ERROR',
                                site=None
                            )
                            retries += 1
                    except (ConnectionError, Timeout) as e:
                        self.logger.log(
                            f"Connection or timeout error occurred while uploading image for post",
                            level='ERROR',
                            site=None,
                            exception=e
                        )
                        retries += 1
                    self.logger.log(
                        f"Retry {retries}/{retry_count}",
                        level='WARNING',
                        site=None
                    )
                self.logger.log(
                    f"Failed to upload image after {retry_count} retries",
                    level='CRITICAL',
                    site=None
                )
        except FileNotFoundError:
            self.logger.log(
                f"File not found: {image_path}",
                level='CRITICAL',
                site=None
            )
        return None 

    def promo_link_html(self, promo_link: str) -> str:
        """
        Generates HTML code for a promotional link section, including buttons for "Home" and optionally a "Watch full video" button.

        Args:
        - promo_link (str): The URL to be used as the promotional link. If `None`, empty, or "-", only the "Home" button is displayed.

        Returns:
        - str: A string of HTML code containing the buttons section.

        Behavior:
        - If `promo_link` is `None`, an empty string, or "-", the generated HTML contains only a "Home" button.
        - If `promo_link` is a valid URL, the generated HTML includes both the "Home" button and a "Watch full video" button that links to the provided URL.
        - The buttons are centered using a flexbox layout.
        """
        layout = {"type":"flex","justifyContent":"center"}
        layout_str = json.dumps(layout)
        if promo_link is None or promo_link == "" or promo_link == "-":
            html_link = f"""<!-- wp:buttons {{"layout":{layout_str}}} -->
            <div class="wp-block-buttons">
            <!-- wp:button -->
            <div class="wp-block-button"><a class="wp-block-button__link wp-element-button" href="http://site.com">Home</a></div>
            <!-- /wp:button -->
            </div><!-- /wp:buttons -->"""
        else:
            html_link = f"""<!-- wp:buttons {{"layout":{layout_str}}} -->
            <div class="wp-block-buttons">
            <!-- wp:button -->
            <div class="wp-block-button"><a class="wp-block-button__link wp-element-button" href="http://site.com">Home</a></div>
            <!-- /wp:button -->
            <!-- wp:button -->
            <div class="wp-block-button"><a class="wp-block-button__link wp-element-button" href="{promo_link}" target="_blank" rel="noreferrer noopener">Watch full video</a></div>
            <!-- /wp:button -->
            </div><!-- /wp:buttons -->"""
        return html_link
    
    def title_html(self, title: str) -> str:
        """
        Generates HTML code for a heading element using the provided title.

        Args:
        - title (str): The text to be displayed as the heading.

        Returns:
        - str: A string of HTML code containing the title wrapped in a `<h2>` heading element.

        Behavior:
        - The function wraps the provided `title` in a `<h2>` HTML heading element with appropriate WordPress block comments.
        - This heading will be rendered as a level-2 heading in the webpage, styled according to the site's CSS.
        """
        html_title = f"<!-- wp:heading --><h2 class='wp-block-heading'>{title}</h2><!-- /wp:heading -->\n"
        return html_title
    
    def video_html(self, video_url: str) -> str:
        """
        Generates HTML code to embed a video player or returns an empty string if the video URL is invalid.

        Args:
        - video_url (str): The URL of the video to be embedded. If `None` or if it starts with 'blob', no video is embedded.

        Returns:
        - str: A string of HTML code for embedding a video player using FluidPlayer, or an empty string if the video URL is not valid.

        Behavior:
        - If `video_url` is `None` or starts with 'blob', the function returns an empty string, indicating no video should be embedded.
        - Otherwise, the function generates HTML code to embed a video player using FluidPlayer, with pre-configured ad rolls and controls.
        - The video player is designed to play a video from the provided `video_url`, with additional settings for autoplay, mute, and ad integration.
        """
        if video_url is None or (isinstance(video_url, str) and video_url.startswith('blob')):
            html_video = ''
        else:
            html_video = f'''
                <!-- wp:html -->
                <script src="https://cdn.fluidplayer.com/v3/current/fluidplayer.min.js"></script>
                <video id="video-id"><source src="{video_url}" type="video/mp4" /></video>
                <script>
                    var myFP = fluidPlayer(
                        'video-id', {{
                        "layoutControls": {{
                            "controlBar": {{
                                "autoHideTimeout": 3,
                                "animated": true,
                                "autoHide": true
                            }},
                            "htmlOnPauseBlock": {{
                                "html": null,
                                "height": null,
                                "width": null
                            }},
                            "autoPlay": false,
                            "mute": false,
                            "allowTheatre": true,
                            "playPauseAnimation": false,
                            "playbackRateEnabled": false,
                            "allowDownload": false,
                            "playButtonShowing": true,
                            "fillToContainer": true,
                            "posterImage": ""
                        }},
                        "vastOptions": {{
                            "adList": [
                                {{
                                    "roll": "preRoll",
                                    "vastTag": "https://go.bbrdbr.com/api/models/vast",
                                    "adText": ""
                                }},
                                {{
                                    "roll": "midRoll",
                                    "vastTag": "https://go.bbrdbr.com/api/models/vast",
                                    "adText": ""
                                }},
                                {{
                                    "roll": "postRoll",
                                    "vastTag": "https://go.bbrdbr.com/api/models/vast",
                                    "adText": ""
                                }}
                            ],
                            "adCTAText": false,
                            "adCTATextPosition": ""
                        }}
                    }});
                </script>
                <!-- /wp:html -->
            '''
        return html_video
