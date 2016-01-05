from collections import OrderedDict
import os
import tempfile


class Project(object):
    def __init__(self, name=None):
        if name is None:
            name = 'project'
        self.name = name

        self._tmp_dir = tempfile.mkdtemp(prefix='metamds_')
        project_dir = os.path.join(self._tmp_dir, self.name)
        os.mkdir(project_dir)
        self.dir = project_dir

        self._tasks = OrderedDict()

    def tasks(self):
        yield from self._tasks.values()

    @property
    def n_tasks(self):
        return len(self._tasks)

    def task_names(self):
        for task in self._tasks:
            yield task.name

    def add_task(self, task):
        if not task.name:
            task.name = 'task_{:d}'.format(self.n_tasks + 1)
        self._tasks[task.name] = task

    def execute(self):
        for task in self.tasks():
            task.execute()
