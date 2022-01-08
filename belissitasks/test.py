import asyncio

from belissitasks import *


class TrivialTaskHandler(AbstractTaskHandler):
    """This TaskHandler just executes any incoming tasks with no args."""

    async def exec_task(self, task: AbstractTask) -> Any:
        return await task.run()


class NumberedTaskHandler(AbstractTaskHandler):
    def __init__(self, number: int):
        self.number = number

    async def exec_task(self, task: AbstractTask) -> Any:
        return await task.run(self.number)


class NumberedTask(AbstractTask):
    def __init__(self, factor: int = 1):
        self.factor = factor

    async def run(self, number: int) -> Any:
        print(f"NUMBER {number} FACTOR {self.factor}")
        await asyncio.sleep(self.factor)

        return number * self.factor


async def main():
    queue = TaskQueue()

    queue.register_task_handler(NumberedTaskHandler(1), NumberedTask)
    queue.register_task_handler(NumberedTaskHandler(2), NumberedTask)

    queue.start()

    out = queue.accept_task(NumberedTask(1))
    out1 = queue.accept_task(NumberedTask(2))
    out2 = queue.accept_task(NumberedTask(3))
    out3 = queue.accept_task(NumberedTask(4))

    print(await asyncio.gather(out, out1, out2, out3))

if __name__ == "__main__":
    asyncio.run(main())
