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
    def __init__(self, dividend: int = 1):
        self.dividend = dividend

    async def run(self, number: int) -> Any:
        print(f"NUMBER {number} DIV {self.dividend}")
        await asyncio.sleep(self.dividend)

        return number / self.dividend


async def main():
    queue = TaskQueue()

    queue.register_task_handler(NumberedTaskHandler(1), NumberedTask)
    queue.register_task_handler(NumberedTaskHandler(2), NumberedTask)

    queue.start()

    print(await asyncio.gather(queue.accept_task(NumberedTask(1)),
                               queue.accept_task(NumberedTask(2)),
                               queue.accept_task(NumberedTask(3)),
                               queue.accept_task(NumberedTask(4)),
                               queue.accept_task(NumberedTask(5)),
                               queue.accept_task(NumberedTask(0))))


if __name__ == "__main__":
    asyncio.run(main())
