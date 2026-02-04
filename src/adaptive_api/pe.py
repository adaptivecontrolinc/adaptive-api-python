from typing import List, Dict, Any, Optional, Union, Callable, Tuple
from dataclasses import dataclass, field
from datetime import datetime, timedelta
import json
import csv
from io import StringIO
from enum import Enum
from dacite import from_dict, Config

import requests

from .base import BaseApi

def to_key_string(key: List[Any]) -> str:
    """Convert a key array to a string representation."""
    j = len(key)
    while j != 0:
        v = key[j - 1]
        if v != '' and v != 0:
            break
        j -= 1
    
    return '@'.join('' if it == ' ' else str(it) for it in key[:j])

def id_to_string(id_value: Any) -> str:
    """Convert an ID to string representation."""
    if id_value is None:
        return ''
    if isinstance(id_value, list):
        return to_key_string(id_value)
    return str(id_value)

def string_to_id(s: str) -> Union[str, List[Union[str, int]]]:
    """Convert a string back to an ID."""
    parts = s.split('@')
    if len(parts) == 1:
        return s
    
    id_parts = []
    for index, value in enumerate(parts):
        if index == 0:
            id_parts.append(' ' if value == '' else value)
        else:
            id_parts.append(int(value))
    
    return id_parts

def id_equals(x: Any, y: Any) -> bool:
    """Check if two IDs are equal."""
    if x == y:
        return True
    
    x_is_array = isinstance(x, list)
    y_is_array = isinstance(y, list)
    
    if x_is_array and y_is_array:
        return len(x) == len(y) and all(xv == yv for xv, yv in zip(x, y))
    
    if x_is_array and not y_is_array:
        return x[0] == y
    
    if not x_is_array and y_is_array:
        return y[0] == x
    
    return False

def contrasting_color(color: str) -> str:
    """Get a contrasting color (black or white) for the given color."""
    if color == '#00000000':
        return '#606060'
    
    rgb = int(color[1:], 16)
    r = (rgb & 0xff0000) // 65536
    g = (rgb & 0xff00) // 256
    b = rgb & 0xff
    
    # https://stackoverflow.com/a/596241
    return '#000000' if (r * 299 + g * 587 + b * 114 > 130000) else '#ffffff'

# Data models

@dataclass
class Command:
    """Command data structure."""
    pass  # Define based on your actual Command structure

@dataclass
class StepRaw:
    """Raw step data structure."""
    pass  # Define based on your actual Step structure

@dataclass
class ProgramSection:
    """Program section data."""
    number: int
    name: str

@dataclass
class Program:
    """Program data structure."""
    number: str
    name: Optional[str] = None
    steps: List[StepRaw] = field(default_factory=list)
    notes: Optional[str] = None
    code: Optional[str] = None
    modifiedTime: Optional[datetime] = None
    modifiedBy: Optional[str] = None

    def __post_init__(self):
        if self.steps is None:
            self.steps = []

@dataclass
class ProgramGroup:
    """Program group data structure."""
    group: str
    programs: List[Program]
    commands: List[Command]
    programSections: Optional[List[ProgramSection]] = None
    messages: Optional[List[str]] = None

@dataclass
class RescheduleResource:
    """Reschedule resource data."""
    name: str

@dataclass
class RescheduleGroup:
    """Reschedule group data."""
    name: str
    resources: List[RescheduleResource]

@dataclass
class DailyJobCount:
    """Daily job count data."""
    day: int
    count: int

@dataclass
class InBoxJob:
    """Inbox job data structure."""
    id: str
    resource: str
    blocked: Optional[bool] = None
    color: Optional[str] = None
    notes: Optional[str] = None
    parameters: Optional[List[Dict[str, str]]] = None
    standardTime: Optional[int] = None
    props: Optional[Dict[str, Any]] = None

@dataclass
class Job(InBoxJob):
    """Job data structure extending InBoxJob."""
    committed: Optional[bool] = None
    start: Optional[int] = None
    end: Optional[int] = None

def item_is_job(item: Union[Job, InBoxJob]) -> Optional[Job]:
    """Check if an item is a Job (has start time)."""
    return item if hasattr(item, 'start') and item.start is not None else None

@dataclass
class InBoxGroupAndJobs:
    """Inbox group and jobs data."""
    group: str
    resources: List[str]
    jobs: Optional[List[InBoxJob]] = None

@dataclass
class ResourceEvent:
    """Base resource event data."""
    start: int
    end: int
    resource: str

@dataclass
class Stoppage(ResourceEvent):
    """Stoppage event data."""
    stoppage: str
    id: str
    notes: Optional[str] = None

def is_stoppage(value: Union[Job, Stoppage]) -> bool:
    """Check if a value is a Stoppage."""
    return hasattr(value, 'stoppage')

@dataclass
class ResourceJobEvent(ResourceEvent):
    """Resource job event data."""
    job: str

@dataclass
class AlarmEvent(ResourceEvent):
    """Alarm event data."""
    alarm: str

@dataclass
class DelayEvent(ResourceEvent):
    """Delay event data."""
    delay: str

SearchResult = Union[Job, InBoxJob]


@dataclass
class AdaptiveHistory:
    """Adaptive history data structure."""
    id: str
    start: datetime
    end: datetime
    elapsedTimes: List[int]
    tags: List['HistoryTag']
    commands: Optional[List[Command]] = None

@dataclass
class Tag:
    """Tag data structure."""
    name: str
    type: Union[str, Dict[str, Any]]
    category: Optional[str] = None
    description: Optional[str] = None
    minimum: Optional[float] = None
    maximum: Optional[float] = None
    io: Optional[Dict[str, Any]] = None
    trace: Optional[Dict[str, Any]] = None
    format: Optional[str] = None

@dataclass
class HistoryTag(Tag):
    """History tag that extends Tag."""
    elapsedIndexes: List[int] = field(default_factory=list)
    values: List[Any] = field(default_factory=list)


class ApiPe(BaseApi):
    def __init__(self, server: str, token: str):
        super().__init__(server, token, "pe")
    """Client for Adaptive PE API."""
    _config = Config(type_hooks={
        datetime: lambda v: datetime.fromisoformat(v.replace('Z', '+00:00')) if isinstance(v, str) else v
    })
    
    
    def _fix_date(self, value: Any) -> Any:
        """Convert date strings to timestamps."""
        if isinstance(value, str):
            try:
                dt = datetime.fromisoformat(value.replace('Z', '+00:00'))
                return int(dt.timestamp() * 1000)
            except ValueError:
                return value
        return value
    
    # API Methods
    
    def program_group_names(self) -> List[str]:
        """Fetch program group names."""
        return self._fetch('programGroupNames')
    
    def program_groups(self, group: Optional[Union[str, List[str]]] = None,
                                  number: Optional[Union[str, List[str]]] = None,
                                  only_step_counts: bool = False) -> List[ProgramGroup]:
        """Fetch program groups."""
        params = {}
        if group is not None:
            params['group'] = group
        if number is not None:
            params['number'] = number
        if only_step_counts:
            params['onlyStepCounts'] = True
        
        data = self._fetch('programGroups', params)
        return [from_dict(ProgramGroup, item, self._config) for item in data]
    
    def history(self, job_id: Any, tags_filter: Optional[str] = None, tags: Optional[List[str]] = None) -> Optional[AdaptiveHistory]:
        """Fetch history for given ID."""
        params = {'id': id_to_string(job_id)}
        if tags_filter:
            params['tagsFilter'] = tags_filter
        if tags:
            params['tags'] = ','.join(tags)
        
        response_json = self._fetch('history', params)
        if not response_json:
            return None
        history = from_dict(AdaptiveHistory, response_json, self._config)        
        _fix_history(history)
        return history


    def reschedule_groups(self) -> List[RescheduleGroup]:
        """Fetch reschedule groups."""
        data = self._fetch('rescheduleGroups')
        return [from_dict(RescheduleGroup, item, self._config) for item in data]
    
    def jobs_and_stoppages(self, after: Optional[int] = None, before: Optional[int] = None,
                           starts_in_range: bool = False, no_jobs: bool = False,
                           no_stoppages: bool = False, job_props: Optional[List[str]] = None) -> List[Union[Job, Stoppage]]:
        """Fetch jobs and stoppages."""
        params = {}
        if after is not None:
            params['after'] = datetime.fromtimestamp(after / 1000).isoformat()
        if before is not None:
            params['before'] = datetime.fromtimestamp(before / 1000).isoformat()
        if starts_in_range:
            params['startsInRange'] = True
        if no_jobs:
            params['noJobs'] = True
        if no_stoppages:
            params['noStoppages'] = True
        if job_props:
            params['jobProps'] = job_props
        
        data = self._fetch('jobs', params)
        
        # Convert dates to numbers
        for item in data:
            item['start'] = self._fix_date(item['start'])
            item['end'] = self._fix_date(item['end'])
        
        # Convert to appropriate objects
        result = []
        for item in data:
            if 'stoppage' in item:
                result.append(from_dict(Stoppage, item, self._config))
            else:
                result.append(from_dict(Job, item, self._config))
        
        return result
    
    def resource_events(self, alarms: bool = False, delays: bool = False,
                        stoppages: bool = False, after: Optional[int] = None,
                        before: Optional[int] = None) -> List[ResourceEvent]:
        """Fetch resource events."""
        params = {}
        if alarms:
            params['alarms'] = True
        if delays:
            params['delays'] = True
        if stoppages:
            params['stoppages'] = True
        if after is not None:
            params['after'] = datetime.fromtimestamp(after / 1000).isoformat()
        if before is not None:
            params['before'] = datetime.fromtimestamp(before / 1000).isoformat()
        
        data = self._fetch('resourceEvents', params)
        
        # Convert dates to numbers
        for item in data:
            item['start'] = self._fix_date(item['start'])
            item['end'] = self._fix_date(item['end'])
        
        return [from_dict(ResourceEvent, item, self._config) for item in data]
    
    def group_resource_events(self, events: Optional[List[ResourceEvent]], 
                             get_name: Callable[[ResourceEvent], str]) -> Optional[Dict[str, Dict[str, int]]]:
        """Group and sum resource events by machine."""
        if not events:
            return None
        
        result = {}
        for event in events:
            name = get_name(event)
            if name not in result:
                result[name] = {}
            
            duration = event.end - event.start
            if event.resource in result[name]:
                result[name][event.resource] += duration
            else:
                result[name][event.resource] = duration
        
        return result
    
    def inbox_jobs(self) -> List[InBoxGroupAndJobs]:
        """Fetch inbox jobs."""
        data = self._fetch('inBoxJobs')
        
        # Repopulate resource in each job
        result = []
        for group_data in data:
            group = from_dict(InBoxGroupAndJobs, group_data, self._config)
            if group.jobs:
                for job in group.jobs:
                    job.resource = group.group
            result.append(group)
        
        return result
    
    def search(self, text: str, limit: Optional[int] = None) -> List[SearchResult]:
        """Search for jobs/items."""
        params = {'text': text}
        if limit is not None:
            params['limit'] = limit
        
        data = self._fetch('search', params)
        
        # Convert dates to numbers
        for item in data:
            if 'start' in item:
                item['start'] = self._fix_date(item['start'])
        
        # Convert to appropriate objects
        result = []
        for item in data:
            if 'start' in item:
                result.append(from_dict(Job, item, self._config))
            else:
                result.append(from_dict(InBoxJob, item, self._config))
        
        return result
    
    def daily_job_count(self) -> List[DailyJobCount]:
        """Fetch daily job count."""
        data = self._fetch('dailyJobCount')
        return [from_dict(DailyJobCount, item, self._config) for item in data]
    
    # Change operations (from apiPeChange.ts)
    
    def insert_jobs(self, inserts: List[Dict[str, Any]]) -> Any:
        """Insert jobs."""
        return self._post('insertJobs', body=json.dumps(inserts))
    
    def update_jobs(self, updates: List[Dict[str, Any]]) -> Any:
        """Update jobs."""
        return self._post('updateJobs', body=json.dumps(updates))
    
    def delete_jobs(self, ids: List[Any]) -> Any:
        """Delete jobs."""
        return self._post('deleteJobs', body=json.dumps(ids))
    
    def insert_programs(self, inserts: List[Dict[str, Any]]) -> Any:
        """Insert programs."""
        return self._post('insertPrograms', body=json.dumps(inserts))
    
    def update_programs(self, updates: List[Dict[str, Any]]) -> Any:
        """Update programs."""
        return self._post('updatePrograms', body=json.dumps(updates))
    
    def delete_programs(self, ids: List[Dict[str, str]]) -> Any:
        """Delete programs."""
        return self._post('deletePrograms', body=json.dumps(ids))
    
def _fix_history(history: AdaptiveHistory) -> None:
    """Fix the history data structure."""
    # The JSON on the wire is in a smaller form that we fix here
    prev = 0
    prev_delta = 0
    elapsed_times = history.elapsedTimes
    for i in range(len(elapsed_times)):
        delta = prev_delta + elapsed_times[i]
        prev_delta = delta
        value = prev + delta
        prev = value
        elapsed_times[i] = value
    for tag in history.tags:
        prev = 0
        elapsed_indexes = tag.elapsedIndexes
        for i in range(len(elapsed_indexes)):
            value = prev + elapsed_indexes[i]
            prev = value
            elapsed_indexes[i] = value
            
        if tag.type == 'number' or tag.type == 'date':
            values = tag.values
            prev = 0
            for i in range(len(values)):
                value = prev + values[i]
                prev = value
                values[i] = value
        elif tag.type == 'boolean':            
            values1 = tag.values
            if len(values1) > 0:
                last_value = values1[0]  # a single initial value
                # Expand to full length by alternating values
                expanded_values = [last_value]
                for i in range(1, len(tag.elapsedIndexes)):
                    last_value = not last_value
                    expanded_values.append(last_value)
                tag.values = expanded_values

def history_to_csv(history: AdaptiveHistory) -> str:
    output = StringIO()
    writer = csv.writer(output, lineterminator='\n')
    
    # Write header
    header = ['ElapsedTime', 'Time'] + [tag.name for tag in history.tags]
    writer.writerow(header)

    num_tags = len(history.tags)
    last_values = [None] * num_tags
    tag_pointers = [0] * num_tags 
    
    # Iterate through every global elapsed time step
    for i in range(len(history.elapsedTimes)):
        row = [
            str(history.elapsedTimes[i]),
            (history.start + timedelta(milliseconds=history.elapsedTimes[i])).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + 'Z'
        ]   
        
        for t_idx, tag in enumerate(history.tags):
            ptr = tag_pointers[t_idx]
            
            # Check if the current global step 'i' matches the tag's next recorded index
            if ptr < len(tag.elapsedIndexes) and tag.elapsedIndexes[ptr] == i:
                # Update the last known value and move the pointer forward
                current_value = tag.values[ptr]
                last_values[t_idx] = current_value
                tag_pointers[t_idx] = ptr + 1
            else:
                # Use the carried-over value (Forward Fill)
                current_value = last_values[t_idx]
            
            row.append(str(current_value) if current_value is not None else '')
            
        writer.writerow(row)
    
    return output.getvalue()
    