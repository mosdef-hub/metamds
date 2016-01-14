from collections import OrderedDict
import os
import tempfile

from metamds import Task


class Simulation(object):
    """

    Parameters
    ----------
    name :
    template :
    project_dir :

    """

    def __init__(self, name=None, template='', project_dir=''):

        if name is None:
            name = 'project'
        self.name = name

        if not project_dir:
            self._tmp_dir = tempfile.mkdtemp(prefix='metamds_')
            project_dir = os.path.join(self._tmp_dir, self.name)
            os.mkdir(project_dir)
        else:
            if not os.path.isdir(project_dir):
                os.mkdir(project_dir)

        self.dir = os.path.abspath(project_dir)

        self._tasks = OrderedDict()

        # if web_address:
        #    download
        # elif python function:
        #
        # elif bunch of strings:
        self.template = template

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

    def parametrize(self, **parameters):
        if hasattr(self.template, '__call__'):
            script = self.template(**parameters)
        # elif isinstance(self.template, tuple_of_strings):
        #     script = list()
        #     for command in self.template:
        #         command.format(**parameters)
        #         script.append(command)

        task = Task(name='ethane', project=self, script=script, input_dir='.')
        self.add_task(task)
        return task