import asyncio

import abc
from collections import defaultdict
from typing import Any


class AbstractTask(abc.ABC):
    priority: int = 0

    @abc.abstractmethod
    async def run(self, *args, **kwargs) -> Any:
        ...


class AbstractTaskHandler(abc.ABC):
    """This is basically a "worker"."""

    @abc.abstractmethod
    async def exec_task(self, task: AbstractTask) -> Any:
        """This function should await the task's run() function with any special args and return its result."""


class TaskQueue:
    def __init__(self):
        self.task_queues: dict[type, list[tuple[AbstractTask, asyncio.Future]]] = defaultdict(lambda: [])
        self.task_handler_queues: dict[type, list[AbstractTaskHandler]] = defaultdict(lambda: [])

        self.update_event = asyncio.Event()
        self.is_running = False

    def register_task_handler(self, task_handler: AbstractTaskHandler, task_type: type):
        """Registers an AbstractTaskHandler in the TaskQueue. It is now used to execute tasks of type task_type."""
        self.task_handler_queues[task_type].append(task_handler)

        # set the event because we changed task_handler_queues
        self.update_event.set()

    def accept_task(self, task: AbstractTask, type_: type = None) -> asyncio.Future:
        """Put a task into the queue and return a Future of its result that can be awaited."""
        type_ = type(task) if type_ is None else type_

        future = asyncio.get_event_loop().create_future()

        # put the task into the queue
        self.task_queues[type_].append((task, future))

        # set the event because we changed task_queues
        self.update_event.set()

        return future

    async def _start_task(self, task: AbstractTask, task_type: type, future: asyncio.Future,
                          task_handler: AbstractTaskHandler | None):
        try:
            # start the task
            if task_handler is None:
                # if no task_handler is specified, await the task directly without a handler
                result = await task.run()
            else:
                result = await task_handler.exec_task(task=task)

            # set the result of the future
            future.set_result(result)
        except Exception as e:
            try:
                raise Exception(f"During the execution of the Task {task}, the above exception occurred.") from e
            except Exception as e:
                future.set_exception(e)
        finally:
            if task_handler is not None:
                # make the task_handler available again
                self.register_task_handler(task_handler, task_type)

    def _update(self):
        for task_type, tasks in self.task_queues.items():
            # sort after priority (higher priority first)
            tasks.sort(key=lambda task_and_future: task_and_future[0].priority, reverse=True)

            if task_type not in self.task_handler_queues:
                # use no task handler, just execute
                for task, future in tasks:
                    asyncio.create_task(self._start_task(task, task_type, future, None))

                # remove the tasks from the queue
                self.task_queues[task_type] = []

                continue

            # available AbstractTaskHandlers will be in task_handler_queues
            for (task, future), task_handler in zip(tasks, self.task_handler_queues[task_type]):
                # schedule the execution of the task in the asyncio event loop
                asyncio.create_task(self._start_task(task, task_type, future, task_handler))

            # remove the task and task handler from their queues, can't do this during the loop as it will somehow stop
            # the iteration
            min_len = min(len(tasks), len(self.task_handler_queues[task_type]))
            del self.task_queues[task_type][:min_len]
            del self.task_handler_queues[task_type][:min_len]

    async def _event_loop(self):
        while self.is_running:
            # wait for a finished task or a change in task_queues or task_handler_queues
            await self.update_event.wait()

            # clear the event to make room for another change
            self.update_event.clear()

            # process
            self._update()

    def start(self):
        """Starts the event loop of the queue."""
        self.is_running = True
        self.update_event.set()

        self._ = asyncio.create_task(self._event_loop())

    def stop(self):
        self.is_running = False
        self.update_event.set()
