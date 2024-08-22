from distribution import scrape
from wp_upload.uploading import upload

def scrape_upload(job_id):
    scrape(job_id)
    upload()
