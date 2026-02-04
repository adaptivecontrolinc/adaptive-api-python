import requests
from typing import List, Dict, Any, Optional, TypedDict
from enum import Enum
from datetime import datetime
from dataclasses import dataclass, field
from dacite import from_dict

from .base import BaseApi

# Enums
class Running(Enum):
    NOT_RUNNING = 0
    RUNNING_BUT_PAUSED = 1
    RUNNING_NOW = 2

class Mode(Enum):
    RUN = 0
    DEBUG = 1
    TEST = 2
    OVERRIDE = 3

# Data models using dataclasses
@dataclass
class LiveMachine:
    machine: str
    type: Optional[str] = None

@dataclass
class DashboardEntry:
    name: str
    lastModified: int
    sizeInBytes: int

@dataclass
class ScreenButton:
    text: str
    svg: Optional[Dict[str, str]] = None  # {viewBox: str, d: str}

@dataclass
class TimelineItem:
    start: int
    end: int
    resource: str
    id: str

@dataclass
class Job:
    id: str
    running: Optional[Dict[str, int]] = None  # {currentStep: int, changingStep?: int}
    blocked: Optional[bool] = None
    committed: Optional[bool] = None
    foregroundColor: Optional[str] = None
    notes: Optional[str] = None
    programs: Optional[List[Any]] = None
    parameters: Optional[List[Dict[str, str]]] = None  # [{command: str}]
    profile: Optional[Dict[str, Any]] = None  # ValueProfile

@dataclass
class ScheduledJob:
    id: str
    start: int
    end: int
    running: Optional[Dict[str, int]] = None
    blocked: Optional[bool] = None
    committed: Optional[bool] = None
    foregroundColor: Optional[str] = None
    notes: Optional[str] = None
    programs: Optional[List[Any]] = None
    parameters: Optional[List[Dict[str, str]]] = None
    profile: Optional[Dict[str, Any]] = None
    standard: Optional[int] = None

@dataclass
class ProgramNumberAndName:
    number: str
    name: str

@dataclass
class Program:
    number: str
    name: Optional[str] = None
    steps: Any = None  # Step[] | number
    notes: Optional[str] = None
    code: Optional[str] = None
    modifiedTime: Optional[datetime] = None
    modifiedBy: Optional[str] = None

@dataclass
class ProgramGroup:
    group: str
    programs: List[Program] = field(default_factory=list)

@dataclass
class SampleStep:
    index: int
    # ... other Step properties

@dataclass
class RunningProfile:
    currentStep: int
    changingStep: int
    sampleSteps: List[SampleStep] = field(default_factory=list)
    # ... other ValueProfile properties

@dataclass
class Shift:
    name: str
    duration: int

@dataclass
class ShiftPattern:
    fromDate: Optional[int]
    repeatPeriodInDays: Optional[int]
    startTime: int
    shifts: List[Shift] = field(default_factory=list)

@dataclass
class GroupNumberAndName:
    group: Optional[str]
    number: str
    name: str

@dataclass
class Tag:
    name: str
    type: Optional[str] = None
    value: Optional[Any] = None
    description: Optional[str] = None

@dataclass
class Command:
    name: str
    description: Optional[str] = None
    parameters: Optional[List[str]] = None

# Type aliases
LiveMachines = List[LiveMachine]
DashboardEntries = List[DashboardEntry]

class ApiLive(BaseApi):
    def __init__(self, server: str, token: str):
        super().__init__(server, token, "live")

    def machines(self, machines: Optional[List[str]] = None) -> LiveMachines:
        """Fetch a list of machines. If 'machines' is provided, fetch only those."""
        data = self._fetch("machines", {"m": machines} if machines else None)
        return [from_dict(LiveMachine, item) for item in data]

    def tag_values_multiple(self, machines: List[str], tags: List[str]) -> Dict[str, List[Any]]:
        """Fetch values for the same tags from multiple machines."""
        return self._fetch("tagValues", {"m": machines, "t": tags})

    def tag_values(self, machine: str, tags: List[str]) -> List[Any]:
        """Fetch values for a single machine and extract the specific data."""
        data = self.tag_values_multiple([machine], tags)
        return data.get(machine, [])

    def tags_multiple(self, machines: List[str]) -> Dict[str, List[Tag]]:
        """Fetch tags for multiple machines."""
        data = self._fetch("tags", {"m": machines})
        return {machine: [from_dict(Tag, tag) for tag in tags] for machine, tags in data.items()}

    def tags(self, machine: str) -> List[Tag]:
        """Fetch tags for a single machine."""
        data = self.tags_multiple([machine])
        return data.get(machine, [])

    def commands_multiple(self, machines: List[str]) -> Dict[str, List[Command]]:
        """Fetch commands for multiple machines."""
        data = self._fetch("commands", {"m": machines})
        return {machine: [from_dict(Command, cmd) for cmd in commands] for machine, commands in data.items()}

    def commands(self, machine: str) -> List[Command]:
        """Fetch commands for a single machine."""
        data = self.commands_multiple([machine])
        return data.get(machine, [])

    def dashboard_entries(self) -> DashboardEntries:
        """Fetch dashboard entries."""
        data = self._fetch("dashboardEntries")
        return [from_dict(DashboardEntry, item) for item in data]

    def dashboard(self, name: str) -> Optional[bytes]:
        """Fetch a dashboard by name, returns binary data or None if not found."""
        url = self._url("dashboard")
        response = self.session.get(url, params={"name": name}, timeout=10)
        if response.status_code == 200:
            return response.content
        return None

    def scene(self, name: str) -> Optional[bytes]:
        """Fetch a scene by name, returns binary data or None if not found."""
        url = self._url("scene")
        response = self.session.get(url, params={"name": name}, timeout=10)
        if response.status_code == 200:
            return response.content
        return None

    def screen_buttons_multiple(self, machines: List[str]) -> Dict[str, List[ScreenButton]]:
        """Fetch screen buttons for multiple machines."""
        data = self._fetch("screenButtons", {"m": machines})
        return {machine: [from_dict(ScreenButton, btn) for btn in buttons] for machine, buttons in data.items()}

    def screen_buttons(self, machine: str) -> List[ScreenButton]:
        """Fetch screen buttons for a single machine."""
        data = self.screen_buttons_multiple([machine])
        return data.get(machine, [])

    def program_groups_multiple(self, machines: List[str], group: Optional[str] = None, only_step_counts: bool = False) -> Dict[str, List[ProgramGroup]]:
        """Fetch program groups for multiple machines."""
        query = {"m": machines}
        if group is not None:
            query["group"] = group
        if only_step_counts:
            query["onlyStepCounts"] = "true"
        data = self._fetch("programs", query)
        
        result = {}
        for machine, groups in data.items():
            result[machine] = []
            for group_data in groups:
                programs = [from_dict(Program, prog) for prog in group_data.get('programs', [])]
                result[machine].append(ProgramGroup(group=group_data['group'], programs=programs))
        return result

    def program_groups(self, machine: str, group: Optional[str] = None, only_step_counts: bool = False) -> List[ProgramGroup]:
        """Fetch program groups for a single machine."""
        data = self.program_groups_multiple([machine], group, only_step_counts)
        return data.get(machine, [])

    def jobs_multiple(self, machines: List[str]) -> Dict[str, List[ScheduledJob]]:
        """Fetch scheduled jobs for multiple machines."""
        data = self._fetch("jobs", {"m": machines})
        return {machine: [from_dict(ScheduledJob, job) for job in jobs] for machine, jobs in data.items()}

    def jobs(self, machine: str) -> List[ScheduledJob]:
        """Fetch scheduled jobs for a single machine."""
        data = self.jobs_multiple([machine])
        return data.get(machine, [])

    def messages_multiple(self, machines: List[str]) -> Dict[str, List[str]]:
        """Fetch messages for multiple machines."""
        return self._fetch("messages", {"m": machines})

    def messages(self, machine: str) -> List[str]:
        """Fetch messages for a single machine."""
        data = self.messages_multiple([machine])
        return data.get(machine, [])

    def profiles(self, machines: List[str]) -> Dict[str, Optional[RunningProfile]]:
        """Fetch running profiles for multiple machines."""
        data = self._fetch("profiles", {"m": machines})
        result = {}
        for machine, profile_data in data.items():
            if profile_data:
                sample_steps = [from_dict(SampleStep, step) for step in profile_data.get('sampleSteps', [])]
                result[machine] = RunningProfile(
                    currentStep=profile_data['currentStep'],
                    changingStep=profile_data['changingStep'],
                    sampleSteps=sample_steps
                )
            else:
                result[machine] = None
        return result

    def screen_multiple(self, machines: List[str], page: Optional[int] = None) -> Dict[str, List[str]]:
        """Fetch screen data for multiple machines."""
        query = {"m": machines}
        if page is not None:
            query["page"] = page
        return self._fetch("screen", query)

    def screen(self, machine: str, page: Optional[int] = None) -> List[str]:
        """Fetch screen data for a single machine."""
        data = self.screen_multiple([machine], page)
        return data.get(machine, [])

    def url_command_icon(self, machine: str, command: str) -> str:
        """Generate URL for command icon."""
        return f"{self._url('commandIcon')}?m={machine}&c={command}"

    # Machine control methods (require change permissions)
    def run(self, machine: str) -> Any:
        """Start/run a machine."""
        return self._post('run', {'m': machine})

    def backward(self, machine: str) -> Any:
        """Move machine backward."""
        return self._post('backward', {'m': machine})

    def forward(self, machine: str) -> Any:
        """Move machine forward."""
        return self._post('forward', {'m': machine})

    def pause(self, machine: str) -> Any:
        """Pause a machine."""
        return self._post('pause', {'m': machine})

    def stop(self, machine: str) -> Any:
        """Stop a machine."""
        return self._post('stop', {'m': machine})

    def yes(self, machine: str) -> Any:
        """Send 'yes' response to machine."""
        return self._post('yes', {'m': machine})

    def no(self, machine: str) -> Any:
        """Send 'no' response to machine."""
        return self._post('no', {'m': machine})

    def set_step(self, machine: str, step: int) -> Any:
        """Set the current step for a machine. Can be fetched in Parent.CurrentStep."""
        return self._post('setStep', {'m': machine, 'step': step})

    def set_mode(self, machine: str, mode: Mode) -> Any:
        """Set the mode for a machine. Can be fetched in Parent.Mode."""
        return self._post('setMode', {'m': machine, 'mode': mode.value})