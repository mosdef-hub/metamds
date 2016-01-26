from collections import OrderedDict
from glob import glob
import os
import tempfile

from metamds import Task
from six import string_types


class Simulation(object):
    """

    Parameters
    ----------
    name :
    template :
    project_dir :

    """

    def __init__(self, name=None, template='', input_dir='', project_dir=''):

        if name is None:
            name = 'project'
        self.name = name
        self._tasks = OrderedDict()
        self.template = template

        if not input_dir:
            self.input_dir = os.getcwd()
        self.input_dir = input_dir

        self.input_files = [f for f in glob('{}/*'.format(self.input_dir))
                            if not f.endswith(('.py', '.ipynb'))]

        if not project_dir:
            self._tmp_dir = tempfile.mkdtemp(prefix='metamds_')
            project_dir = os.path.join(self._tmp_dir, self.name)
            os.mkdir(project_dir)
        else:
            if not os.path.isdir(project_dir):
                os.mkdir(project_dir)

        self.dir = os.path.abspath(project_dir)

    def tasks(self):
        for v in self._tasks.values():
            yield v

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
        parameters['input_dir'] = self.input_dir
        if hasattr(self.template, '__call__'):
            script = self.template(**parameters)
        # elif is_url(self.template):
        #     treat as blockly and download from github
        elif _is_iterable_of_strings(self.template):
            script = list()
            for command in self.template:
                command.format(**parameters)
                script.append(command)
        else:
            script = None

        if not _is_iterable_of_strings(script):
            raise ValueError('Unusable template: {}\n Templates should either '
                             'be an iterable of strings or a function that '
                             'returns an iterable of strings.'.format(self.template))

        task = Task(project=self, script=script)
        self.add_task(task)
        return task


def _is_iterable_of_strings(script):
    try:
        return all(isinstance(line, string_types) for line in script)
    except:
        return False
