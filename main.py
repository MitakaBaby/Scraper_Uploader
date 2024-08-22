from jobs import scrape_upload
from scheduler import Scheduler
import time

scheduler = Scheduler()

if __name__ == "__main__":

    scheduler.every().day.at("12:10").with_id("Sites at 12:00").do(scrape_upload, "Sites at 12:00")
    scheduler.every().hour.with_id("Not sorted").do(scrape_upload, "Not sorted")
    while True:
        scheduler.run_pending()
        time.sleep(5)
