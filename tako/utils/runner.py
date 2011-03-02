# -*- Mode: Python; tab-width: 4; indent-tabs-mode: nil; -*-

from syncless import coio
from syncless.util import Queue

class Task(object):
    """docstring for Task"""
    def __init__(self, f, *args, **kwargs):
        super(Task, self).__init__()
        self.f = f
        self.args = args
        self.kwargs = kwargs

    def __call__(self):
        return self.f(*self.args, **self.kwargs)

class Runner(object):
    def __init__(self, tasks):
        super(Runner, self).__init__()
        self.tasks = tasks
        self.completed_count = 0
        self.q = Queue()
        self.results = [None for task in tasks]

    def spawn(self):
        i = 0
        for task in self.tasks:
            assert task
            coio.stackless.tasklet(self._task_runner)(task, i)
            i += 1

    def await_completion(self):
        self.q.pop()

    def _task_runner(self, task, i):
        result = task()
        self._complete(result, i)

    def _complete(self, result, i):
        self.results[i] = result
        self.completed_count += 1
        if self.completed_count == len(self.tasks):
            self.q.append(True)

def run(tasks):
    r = Runner(tasks)
    r.spawn()
    r.await_completion()
    return r.results

def task(func, *args, **kwargs):
    assert func
    return Task(func, *args, **kwargs)

def spawn(func, *args, **kwargs):
    return coio.stackless.tasklet(func)(*args, **kwargs)

if __name__ == '__main__':

    def foo(bar):
        print bar

    run([task(foo, 'hello foo')])
