import sys, time, inspect
from pathlib import Path
from collections.abc import Callable

default_LoSs = {
    "UNSET": 0,
    "DEBUG": 10,
    "INFO": 20,
    "WARN": 30,
    "ERROR": 40,
    "FATAL": 50
}

class Logger:
    def __init__(self,
                 name: str,
                 file: str | Path = None,
                 reset_file_on_restart: bool = True,
                 console = sys.stdout,
                 min_console_severity: int = 0,
                 min_file_severity: int = 0,
                 time_format: str = "%Y-%m-%d %H:%M:%S",
                 LoSs: dict[str, int] = default_LoSs,
                 ):
        self.reset_file_on_restart = reset_file_on_restart
        self.name = name
        self.console = console
        self.file = file
        self.time_format = time_format
        self.LoSs = LoSs
        self.RLoSs = {v: k for k, v in LoSs.items()}
        self.min_console_severity = min_console_severity
        self.min_file_severity = min_file_severity

        if reset_file_on_restart and file:
            with open(file, "w") as f:
                f.write("")

    def add_LoS(self, name: str, severity: int):
        self.LoSs[name] = severity
        self.RLoSs[severity] = name

    def _premessage(self, severity):
        tm = time.strftime(self.time_format)
        return f"{tm} [{self.name}/{self.RLoSs[severity]}]: "

    async def alog(self, severity: int, content: str, secondary: Callable = None, kwarg: str = None):
        """Async compatible implementation of `log`"""
        if isinstance(severity, str):
            severity = self.LoSs[severity]

        content = f"{self._premessage(severity)}{content}\n"

        if secondary and inspect.iscoroutinefunction(secondary):
            if kwarg:
                await secondary(**{kwarg: content})
            else:
                await secondary(content)
        elif secondary:
            if kwarg:
                secondary(**{kwarg: content})
            else:
                secondary(content)

        if (severity >= self.min_console_severity) and (self.console):
            self.console.write(content)
            if inspect.iscoroutinefunction(self.console.flush):
                await self.console.flush()
            else:
                self.console.flush()
        
        if (severity >= self.min_file_severity) and (self.file):
            with open(self.file, "a") as file:
                file.write(content)
        
    def log(self, severity: int, content: str, secondary: Callable = None, kwarg: str = None):
        if isinstance(severity, str):
            severity = self.LoSs[severity]

        content = f"{self._premessage(severity)}{content}\n"
            
        if secondary:
            if kwarg:
                secondary(**{kwarg: content})
            else:
                secondary(content)
        if (severity >= self.min_console_severity) and (self.console):
            self.console.write(content)
            self.console.flush()

        if (severity >= self.min_file_severity) and (self.file):
            with open(self.file, "a") as file:
                file.write(content)

    def debug(self, content: str, secondary: Callable = None, kwarg: str = None):
        self.log(self.LoSs["DEBUG"], content, secondary, kwarg)

    def info(self, content: str, secondary: Callable = None, kwarg: str = None):
        self.log(self.LoSs["INFO"], content, secondary, kwarg)

    def warn(self, content: str, secondary: Callable = None, kwarg: str = None):
        self.log(self.LoSs["WARN"], content, secondary, kwarg)

    def error(self, content: str, secondary: Callable = None, kwarg: str = None):
        self.log(self.LoSs["ERROR"], content, secondary, kwarg)

    def fatal(self, content: str, secondary: Callable = None, kwarg: str = None):
        self.log(self.LoSs["FATAL"], content, secondary, kwarg)

    async def adebug(self, content: str, secondary: Callable = None, kwarg: str = None):
        await self.alog(self.LoSs["DEBUG"], content, secondary, kwarg)

    async def ainfo(self, content: str, secondary: Callable = None, kwarg: str = None):
        await self.alog(self.LoSs["INFO"], content, secondary, kwarg)

    async def awarn(self, content: str, secondary: Callable = None, kwarg: str = None):
        await self.alog(self.LoSs["WARN"], content, secondary, kwarg)

    async def aerror(self, content: str, secondary: Callable = None, kwarg: str = None):
        await self.alog(self.LoSs["ERROR"], content, secondary, kwarg)

    async def afatal(self, content: str, secondary: Callable = None, kwarg: str = None):
        await self.alog(self.LoSs["FATAL"], content, secondary, kwarg)
