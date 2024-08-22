import os
import uuid
import json
import functools
from datetime import datetime, timedelta
from typing import Optional, Callable, List

from common import CustomLogger, Utils, Paths, Popups

logger = CustomLogger()

class ScheduleError(Exception):
    """Base schedule exception"""

    pass


class ScheduleValueError(ScheduleError):
    """Base schedule value error"""

    pass


class IntervalError(ScheduleValueError):
    """An improper interval was used"""

    pass


class Scheduler:
    """
    
    """
    def __init__(self) -> None:
        """
        Initializes a Scheduler instance.
        """
        self.jobs: List[Job] = []
        self.data_file = os.path.join(Paths().jsons, 'scheduler_data.json')       

    def save_data(self) -> None:
        """
        Saves job data from self.jobs to the JSON file.

        Only updates the specific jobs in the JSON file.
        """
        existing_jobs_dict = {}
        if os.path.exists(self.data_file):
            try:
                with open(self.data_file, 'r', encoding='utf-8') as f:
                    existing_data = json.load(f)
                    existing_jobs = existing_data.get('jobs', [])
                    existing_jobs_dict = {job_data['id']: job_data for job_data in existing_jobs}
            except (json.JSONDecodeError, ValueError) as e:
                logger.log(f"Failed to load data from {self.data_file}",
                            level='CRITICAL',
                            site="SCHEDULER",
                            exception=e)

        updated_jobs_dict = {job.job_id: job.to_dict() for job in self.jobs}

        for job_id, job_data in updated_jobs_dict.items():
            existing_jobs_dict[job_id] = job_data

        updated_job_list = list(existing_jobs_dict.values())

        required_space = len(json.dumps({'jobs': updated_job_list}))
        while True:
            if Utils().check_disk_space(os.path.dirname(self.data_file), required_space):
                try:
                    with open(self.data_file, 'w', encoding='utf-8') as f:
                        data = {
                            'jobs': updated_job_list
                        }
                        json.dump(data, f, default=str, indent=4)
                    break
                except IOError as e:
                    if e.errno == 28:
                        logger.log(f"Failed to save data to {self.data_file} due to insufficient disk space",
                                    level='CRITICAL',
                                    site="SCHEDULER",
                                    exception=e)
                        Popups().space_error_popup(lambda: None)
                    else:
                        logger.log(f"Failed to save data to {self.data_file} due to an unexpected I/O error",
                                    level='CRITICAL',
                                    site="SCHEDULER",
                                    exception=e)
                        raise
            else:
                Popups().space_error_popup(lambda: None)

    def every(self, interval=1) -> "Job":
        """
        Schedule a new periodic job.

        Args:
        - interval (int): A quantity of a certain time unit

        Returns:
        - JobBuilder: An unconfigured :class:`Job <Job>`
        """
        job = Job(interval, self)
        return job

    def _run_job(self, job: "Job") -> None:
        job.run()

    def run_pending(self) -> None:
        """
        Runs jobs that are due for execution.

        Iterates through self.jobs and executes jobs whose next_run
        time is less than or equal to the current time. Saves job data
        after executing each job.
        """
        current_time = datetime.strptime(Utils.get_current_datetime(), "%Y-%m-%d %H-%M-%S")

        for job in self.jobs:
            if job.next_run and job.next_run <= current_time:
                logger.log(f"JOB: {job.job_id}, Running task scheduled at {job.next_run}",
                    level='INFO',
                    site="SCHEDULER")
                self._run_job(job)
                self.save_data()


class Job:
    """
    
    """
    def __init__(self, interval: int, scheduler: Optional[Scheduler] = None) -> None:
        """
        Initializes a Job instance.

        Args:
        - interval (int): Interval between job executions.
        - scheduler (Scheduler, optional): Scheduler instance to add the job to.

        If scheduler is not provided, it will be initialized with a new Scheduler instance.
        Sets attributes for job_id, interval, unit, job_func, at_time, day_of_week, last_run, and next_run.
        """
        self.job_id = str if str else self.generate_id()
        self.interval = interval
        self.unit = int
        self.job_func: Optional[functools.partial] = None
        self.at_time = None
        self.day_of_week = None
        self.last_run = None
        self.next_run = None
        self.args = None
        self.kwargs = None
        self.scheduler: Optional[Scheduler] = scheduler
        self.data_file = os.path.join(Paths().jsons, 'scheduler_data.json')

    @staticmethod
    def generate_id():
        """
        Generates random ID for the Job.
        """
        return f"{uuid.uuid4()}"

    @property
    def second(self):
        if self.interval != 1:
            raise IntervalError("Use seconds instead of second")
        return self.seconds

    @property
    def seconds(self):
        self.unit = "seconds"
        return self

    @property
    def minute(self):
        if self.interval != 1:
            raise IntervalError("Use minutes instead of minute")
        return self.minutes

    @property
    def minutes(self):
        self.unit = "minutes"
        return self

    @property
    def hour(self):
        if self.interval != 1:
            raise IntervalError("Use hours instead of hour")
        return self.hours

    @property
    def hours(self):
        self.unit = "hours"
        return self

    @property
    def day(self):
        if self.interval != 1:
            raise IntervalError("Use days instead of day")
        return self.days

    @property
    def days(self):
        self.unit = "days"
        return self

    @property
    def week(self):
        if self.interval != 1:
            raise IntervalError("Use weeks instead of week")
        return self.weeks

    @property
    def weeks(self):
        self.unit = "weeks"
        return self

    def at(self, time_str: str) -> 'Job':
        """
        Sets the specific time of day for job execution.

        Args:
        - time_str (str): Time string in the format 'HH:MM'.

        Returns:
        - JobBuilder: Instance of JobBuilder with at_time set to time_str.
        """
        self.at_time = time_str
        return self
    
    def on(self, day) -> 'Job':
        """
        Sets the specific day of the week for job execution.

        Args:
        - day (str or int): Day of the week, either as a string (e.g., 'Monday') or an integer (1 for Monday, 2 for Tuesday, etc.).

        Returns:
        - JobBuilder: Instance of JobBuilder with day set for weekly scheduling.
        """
        self.day_of_week = day
        return self

    def with_id(self, id) -> 'Job':
        """
        Sets an ID for the job.

        Args:
        - id (str): ID for identifying the job uniquely.

        Returns:
        - JobBuilder: Instance of JobBuilder with ID set.
        """
        self.job_id = id
        return self

    def do(self, job_func: Callable, *args, **kwargs):
        """
        Specifies the job_func that should be called every time the
        job runs.

        Any additional arguments are passed on to job_func when
        the job runs.

        :param job_func: The function to be scheduled
        :return: The invoked job instance
        """
        self.job_func = functools.partial(job_func, *args, **kwargs)
        self.calculate_next_run()
        functools.update_wrapper(self.job_func, job_func)

        if self.scheduler is None:
            raise ScheduleError(
                "Unable to add job to schedule. Job is not associated with a scheduler"
            )
        if not isinstance(self.job_id, str):
            raise ValueError("job_id must be a string")

        job_data = self.load_job_data(self.job_id, self.data_file)
        if job_data:
            job = self.from_dict(job_data)
            self.scheduler.jobs.append(job)
        else:
            self.scheduler.jobs.append(self)
        
        return self

    @staticmethod
    def load_job_data(job_id: str, file_path: str):
        """
        Loads job data from a JSON file and returns the job data if the job_id matches.

        Args:
        - job_id (str): The ID of the job to look for.
        - file_path (str): The path to the JSON file.

        Returns:
        - dict: Job data if found, otherwise None.
        """
        if not os.path.isfile(file_path):
            logger.log(f"File not found: {file_path}", level='WARNING', site="SCHEDULER")
            return None

        with open(file_path, 'r') as file:
            try:
                content = file.read().strip()
                if not content:
                    logger.log(f"File is empty: {file_path}", level='WARNING', site="SCHEDULER")
                    return None
                
                data = json.loads(content)
                jobs_data = data.get('jobs', [])
            except json.JSONDecodeError as e:
                logger.log(f"Error decoding JSON from file: {e}", level='CRITICAL', site="SCHEDULER")
                return None

        if isinstance(jobs_data, list):
            for job_data in jobs_data:
                if isinstance(job_data, dict) and job_data.get('id') == job_id:
                    return job_data
        
        return None

    def calculate_next_run(self) -> None:
        """
        Calculates the next scheduled run time of the job.

        If unit is 'days', combines the current date with at_time.
        Adjusts next_run if the current time is greater than next_run.
        """
        if self.unit == 'days':
            run_time = datetime.strptime(self.at_time, '%H:%M').time() if self.at_time else datetime.strptime(Utils.get_current_time(), "%H:%M").time()
            next_run = datetime.combine(datetime.strptime(Utils.get_current_date(), "%b %d, %Y"), run_time)
            if self.last_run is None:
                next_run = next_run
            elif datetime.strptime(Utils.get_current_datetime(), "%Y-%m-%d %H-%M-%S") > next_run:
                next_run += timedelta(days=self.interval)
            self.next_run = next_run

        elif self.unit == 'weeks':
            if self.day_of_week is None:
                raise ValueError("day_of_week must be set for weekly scheduling")
            
            if isinstance(self.day_of_week, int):
                day_of_week = self.day_of_week
            else:
                weekdays = ['monday', 'tuesday', 'wednesday', 'thursday', 'friday', 'saturday', 'sunday']
                try:
                    day_of_week = weekdays.index(self.day_of_week.lower())
                except ValueError:
                    raise ValueError(f"Invalid day_of_week: {self.day_of_week}")

            next_run = datetime.strptime(Utils.get_current_datetime(), "%Y-%m-%d %H-%M-%S") + timedelta((day_of_week - datetime.strptime(Utils.get_current_datetime(), "%Y-%m-%d %H-%M-%S").weekday()) % 7)
            if datetime.strptime(Utils.get_current_datetime(), "%Y-%m-%d %H-%M-%S") > next_run:
                next_run += timedelta(weeks=self.interval)
            self.next_run = next_run

        elif self.unit == 'hours':

            if self.last_run is None:
                next_run = datetime.combine(datetime.now().date(), datetime.strptime(Utils.get_current_time(), "%H:%M:%S").time()) + timedelta(hours=self.interval)
            else:
                last_run_datetime = datetime.combine(self.last_run.date(), self.last_run.time())
                next_run = last_run_datetime + timedelta(hours=self.interval)

            if next_run <= datetime.now():
                next_run += timedelta(days=1)

            self.next_run = next_run

        else:
            raise ValueError(f"Unsupported unit: {self.unit}")
        
    def run(self) -> None:
        """
        Executes the job's action function.

        Sets last_run to the current time.
        Invokes the action function.
        Calculates the next run time using calculate_next_run().
        Logs the task execution time and script name.
        """
        if self.job_func is None:
            raise ValueError("Cannot run job because job_func is None")
        
        ret = self.job_func()

        self.last_run = datetime.strptime(Utils.get_current_datetime(), "%Y-%m-%d %H-%M-%S")
        self.calculate_next_run()

        return ret

    def to_dict(self) -> dict:
        """
        Converts the Job instance to a dictionary format.

        Returns:
        - dict: Dictionary representation of the Job instance.
        """
        if isinstance(self.job_func, functools.partial):
            if hasattr(self.job_func.func, '__module__'):
                action_module = self.job_func.func.__module__
            if hasattr(self.job_func.func, '__name__'):
                action_name = self.job_func.func.__name__
        elif self.job_func:
            if hasattr(self.job_func, '__module__'):
                action_module = self.job_func.__module__
            if hasattr(self.job_func, '__name__'):
                action_name = self.job_func.__name__
        
        return {
            'id': self.job_id,
            'interval': self.interval,
            'unit': self.unit,
            'at_time': self.at_time if self.at_time else None,
            'day_of_week': self.day_of_week,
            'last_run': self.last_run.isoformat() if self.last_run else None,
            'next_run': self.next_run.isoformat() if self.next_run else None,
            'action_module': action_module,
            'action_name': action_name,
            'args': self.job_func.args if isinstance(self.job_func, functools.partial) else (),
            'kwargs': self.job_func.keywords if isinstance(self.job_func, functools.partial) else {},
        }

    @classmethod
    def from_dict(cls, data):
        """
        Creates a Job instance from a dictionary.

        Args:
        - data (dict): Dictionary containing job data.

        Returns:
        - Job: Job instance created from the dictionary.
        """
        action_module = data['action_module']
        action_name = data['action_name']
        action = getattr(__import__(action_module), action_name)
        
        job = cls(
            data['interval']
        )
        job.job_id = data['id']
        job.unit = data['unit']
        job.at_time = data.get('at_time')
        job.day_of_week = data.get('day_of_week')
        if data['last_run']:
            job.last_run = datetime.fromisoformat(data['last_run'])
        if data['next_run']:
            job.next_run = datetime.fromisoformat(data['next_run'])
        else:
            job.calculate_next_run()

        if 'args' in data or 'kwargs' in data:
            args = data.get('args', ())
            kwargs = data.get('kwargs', {})
            job.job_func = functools.partial(action, *args, **kwargs)
            functools.update_wrapper(job.job_func, action)
        else:
            job.job_func = action
        
        return job
