import os
import json
from datetime import datetime


from common import Utils, CustomLogger, Paths, Jsons
from scrapemethods import Methods


logger = CustomLogger()

def sites_to_run(job_id):

    day_mapping = {
        0: "Monday",
        1: "Tuesday",
        2: "Wednesday",
        3: "Thursday",
        4: "Friday",
        5: "Saturday",
        6: "Sunday",
    }

    site_lists = {
        "Sites at 12:00": {
            "Monday": [],
            "Tuesday": [],
            "Wednesday": [],
            "Thursday": [],
            "Friday": [],
            "Saturday": [],
            "Sunday": [],
            "Daily": [
                "site2412",
                "testy1233", "site32121", "site0120",
                "somethingsomethin291", 
                ]
        },
        "Not sorted":{
            "Monday": [],
            "Tuesday": ['site3'],
            "Wednesday": [],
            "Thursday": [],
            "Friday": [],
            "Saturday": [],
            "Sunday": [],
            "Daily": ['site1', 'site2']
        }
    }
    schedule_time = None
    sites_to_run = []

    data_file = os.path.join(Paths().jsons, 'scheduler_data.json')
    if os.path.exists(data_file):
        with open(data_file, 'r', encoding='utf-8') as f:
            content = f.read().strip()
            if not content:
                logger.log(f"File is empty: {data_file}",
                            level='WARNING',
                            site="SCRAPER"
                            )
                return sites_to_run

            try:
                data = json.loads(content)
                schedules = data.get('jobs', [])
            except json.JSONDecodeError as e:
                logger.log(f"Error decoding JSON from file {data_file}: {e}", level='CRITICAL', site="SCRAPER")
                return sites_to_run
    else:
        logger.log(f"File not found: {data_file}",
                   level='WARNING',
                   site="SCRAPER")
        return sites_to_run

    current_day = day_mapping.get(Utils.get_day_of_week())
    current_time = datetime.strptime(Utils.get_current_time(), "%H:%M:%S")

    def should_run_today(schedule, schedule_time, current_time):
        try:
            next_run_datetime = datetime.fromisoformat(schedule.get('next_run'))
            return current_time > schedule_time and next_run_datetime.date() == datetime.now().date()
        except ValueError:
            return False
        
    schedules_dict = {schedule['id']: schedule for schedule in schedules}
    schedule = schedules_dict.get(job_id)

    if schedule:
        at_time = schedule.get('at_time')
        if at_time:
            try:
                schedule_time = datetime.strptime(at_time, '%H:%M')
            except ValueError as ve:
                schedule_time = None
                logger.log(f"Invalid 'at_time' format for job_id {job_id}",
                    level='ERROR',
                    site=None,
                    exception=ve
                    )

    for list_name, day_list in site_lists.items():
        if current_day in day_list:
            if schedule_time and should_run_today(schedule, schedule_time, current_time):
                sites_list_for_current_day = day_list.get(current_day, [])
                daily_sites = day_list.get("Daily", [])
                if list_name == job_id and current_time > schedule_time:
                    if daily_sites:
                        sites_to_run.extend(daily_sites)
                    if sites_list_for_current_day:
                        sites_to_run.extend(sites_list_for_current_day)
            elif not schedule_time:
                sites_list_for_current_day = day_list.get(current_day, [])
                daily_sites = day_list.get("Daily", [])
                if list_name == job_id:
                    if daily_sites:
                        sites_to_run.extend(daily_sites)
                    if sites_list_for_current_day:
                        sites_to_run.extend(sites_list_for_current_day)

    return sites_to_run

#remove the hashtags bellow for performance testing
import gc
import tracemalloc
import psutil

def memory_usage():
    process = psutil.Process(os.getpid())
    memory_info = process.memory_info()
    logger.log(f"RSS: {memory_info.rss / (1024 * 1024):.2f} MB",
        level='DEBUG',
        site=None
        )
    logger.log(f"VMS: {memory_info.vms / (1024 * 1024):.2f} MB",
        level='DEBUG',
        site=None
        )

def top_memory(snapshot_diff, key_type='lineno', limit=10):
    logger.log(f"Top {limit} lines",
        level='DEBUG',
        site=None
        )
    for index, stat in enumerate(snapshot_diff[:limit], 1):
        logger.log(f"#{index}: {stat.traceback.format()}: {stat.size_diff / 1024:.1f} KiB",
            level='DEBUG',
            site=None
            )
        frame = stat.traceback[0]
        if frame.filename.startswith("<frozen"):
            line = "<internal Python module>"
        else:
            try:
                lines = open(frame.filename).readlines()
                line = lines[frame.lineno - 1].strip()
            except OSError:
                line = "<could not retrieve source>"

        if line:
            logger.log(f"    {line}",
                level='DEBUG',
                site=None
                )

    other = snapshot_diff[limit:]
    if other:
        size = sum(stat.size_diff for stat in other)
        logger.log(f"{len(other)} other: {size / 1024:.1f} KiB",
            level='DEBUG',
            site=None
            )
    total = sum(stat.size_diff for stat in snapshot_diff)
    logger.log(f"Total allocated size: {total / 1024:.1f} KiB",
        level='DEBUG',
        site=None
        )

def scrape(job_id):
    all_sites = ["site1", "site2", "site3"]
    
    #sites = ["site1"]

    sites = sites_to_run(job_id)
    for site in sites:
        #memory_usage()
        #tracemalloc.start()
        #snapshot1 = tracemalloc.take_snapshot()
        
        config = Jsons.load_configs(site)
        method_name = config.get("scrape_method")
        if method_name:
            site_processor = Methods(site)
            if hasattr(site_processor, method_name):
                try:
                    method_to_call = getattr(site_processor, method_name)
                    method_to_call(site)
                except Exception as e:
                    logger.log("Error is",
                        level='CRITICAL',
                        site=site,
                        exception=e)
                    continue
        

        #gc.collect()

        #snapshot2 = tracemalloc.take_snapshot()
        #top_memory(snapshot2.compare_to(snapshot1, 'lineno'))

        #tracemalloc.stop()
        #memory_usage()

if __name__ == "__main__":
    #testing only
    job_id = ""

    scrape(job_id)
