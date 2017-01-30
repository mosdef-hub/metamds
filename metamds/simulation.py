from collections import OrderedDict
from glob import glob
import logging
import os
import tempfile

from six import string_types

from metamds import Task
from metamds.io import rsync_to
from metamds.db import add_doc_db


class Simulation(object):
    """

    Attributes
    ----------
    name :
    tasks :
    template :
    output_dir :
    input_dir :
    remote_dir :
    info :
    debug :

    """

    def __init__(self, name=None, template='', output_dir='', input_dir=''):

        if name is None:
            name = 'project'
        self.name = name
        self._tasks = OrderedDict()
        self.template = template

        if not input_dir:
            self.input_dir = os.getcwd()
        self.input_dir = os.path.abspath(input_dir)

        if not output_dir:
            self._tmp_dir = tempfile.mkdtemp(prefix='metamds_')
            output_dir = os.path.join(self._tmp_dir, self.name)
            os.mkdir(output_dir)
        else:
            if not os.path.isdir(output_dir):
                os.mkdir(output_dir)
        self.output_dir = os.path.abspath(output_dir)

        self.input_files = [f for f in glob('{}/*'.format(self.input_dir))
                            if not f.endswith(('.py', '.ipynb')) and
                            f != self.output_dir]

        self.remote_dir = None

        self.info = logging.getLogger('{}_info'.format(self.name))
        self.info.setLevel(logging.INFO)
        log_file = os.path.join(self.output_dir, '{}_info.log'.format(self.name))
        handler = logging.FileHandler(log_file)
        formatter = logging.Formatter('[%(asctime)s] %(levelname)s - %(message)s')
        handler.setFormatter(formatter)
        self.info.addHandler(handler)

        self.debug = logging.getLogger('{}_debug'.format(self.name))
        self.debug.setLevel(logging.DEBUG)
        log_file = os.path.join(self.output_dir, '{}_debug.log'.format(self.name))
        handler = logging.FileHandler(log_file)
        formatter = logging.Formatter('[%(asctime)s] %(levelname)s - %(message)s')
        handler.setFormatter(formatter)
        self.debug.addHandler(handler)

    def create_remote_dir(self, client, hostname, username):
        """Create a copy of all input files and `output_dir` on a remote host.

        Parameters
        ----------
        client : paramiko.SSHClient

        """
        if not self.remote_dir:
            #---------------------------------------------------------------- TJ
            # For different resources it is necessary to move to preferred production directories
            # TODO: Add as variable somewhere rather than hardcoding
            if "nersc" in hostname:
                _, stdout, stderr = client.exec_command('cd $SCRATCH; mktemp -d; pwd')
            elif "accre" in hostname:
                _, stdout, stderr = client.exec_command('cd /scratch/{}; mktemp -d; pwd'.format(username))
            if "rahman" in hostname:
                _, stdout, stderr = client.exec_command('mktemp -d; pwd')
            #---------------------------------------------------------------- TJ 
            if stderr.readlines():
                raise IOError(stderr.read().decode('utf-8'))
            remote_dir, home = (line.rstrip() for line in stdout.readlines())
            # TODO: tidy up temp dir creation and copying
            self.remote_dir = os.path.join(home, remote_dir[5:])

            cmd = 'rsync -r {tmp_dir} ~'.format(tmp_dir=remote_dir)
            _, stdout, stderr = client.exec_command(cmd)
            if stderr.readlines():
                raise IOError(stderr.read().decode('utf-8'))

        # Move input files
        rsync_to(flags='-r -h --progress --partial',
                 src=' '.join(self.input_files),
                 dst=self.remote_dir,
                 user=client.username,
                 host=client.hostname,
                 logger=self.debug)
        # Move output directory including relative symlinks to input files
        rsync_to(flags='-r -h --links --progress --partial',
                 src=self.output_dir,
                 dst=self.remote_dir,
                 user=client.username,
                 host=client.hostname,
                 logger=self.debug)

    def tasks(self):
        """Yield all tasks in this simulation. """
        for v in self._tasks.values():
            yield v

    @property
    def n_tasks(self):
        """Return the number of tasks in this simulation. """
        return len(self._tasks)

    def task_names(self):
        """Return the names of all tasks in this simulation. """
        for task in self._tasks:
            yield task.name

    def add_task(self, task):
        """Add a task to this simulation. """
        if not task.name:
            task.name = 'task_{:d}'.format(self.n_tasks + 1)
        self._tasks[task.name] = task

    def execute_all(self, hostname=None, username=None):
        """Execute all tasks in this simulation. """
        for task in self.tasks():
            task.execute(hostname=hostname, username=username)

    def sync_all(self):
        for task in self.tasks():
            task.sync()

    def parametrize(self, **parameters):
        """Parametrize and add a task to this simulation. """
        task = Task(simulation=self)

        parameters['input_dir'] = os.path.relpath(self.input_dir, task.output_dir)

        cwd = os.getcwd()
        os.chdir(task.output_dir)
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

        os.chdir(cwd)
        # Parametrizing a task can and typically will produce input files.
        self.input_files = [f for f in glob('{}/*'.format(self.input_dir))
                            if not f.endswith(('.py', '.ipynb')) and
                            f != self.output_dir]
        task.script = script
        self.add_task(task)
        return task

    def add_to_db(self, host="127.0.0.1", port=27017, database="shearing_simulations", 
                  user=None, password=None, collection="tasks", use_full_uri=False, 
                  update_duplicates=False, **parameters):
        """Adds simulation parameters and io file locations to db.
        
        Parameters
        ----------
        host : str, optional
            database connection host (the default is 127.0.0.1, or the local computer being used)
        port : int, optional
            database host port (default is 27017, which is the pymongo default port).
        database : str, optional
            name of the database being used (default is shearing_simulations).
        user : str, optional 
            user name (default is None, meaning the database is public).
        password : str, optional 
            user password (default is None, meaning there is no password access to database).
        collection : str, optional 
            database collection name for doc location (default is tasks).
        use_full_uri : bool, optional 
            optional use of full uri path name, necessary for hosted database (default is False,
            meaning the files being used in the database are local).
        update_duplicates : bool, optional
            determines ifduplicates in the database will be updated (default is False, meaning 
            the added doc should not replace an existing doc that is equivalent)
        **parameters : dict, optional
            keys and fields added in doc.
        """
        # TODO:: add user//pw functionality when MongoDB is hosted
        
        for key in parameters:
            if type(parameters[key]).__name__ in ['function', 'type']:
                parameters[key] = parameters[key].__name__
       
        if use_full_uri:
            output_dir = get_uri("{}/task_{:d}/".format(self.output_dir, self.n_tasks-1))
            input_dir = get_uri(self.input_dir)
            input_files = get_uri(self.input_files)
        else:
            output_dir = "{}/task_{:d}/".format(self.output_dir, self.n_tasks-1)
            input_dir = self.input_dir
            input_files = self.input_files

        parameters['output_dir'] = output_dir
        parameters['input_dir'] = input_dir
        parameters['input_files'] = input_files
        
        add_doc_db(host=host, port=port,database=database, user=user, password=password,
                   collection=collection, doc=parameters, 
                   update_duplicates=update_duplicates) 

def _is_iterable_of_strings(script):
    try:
        return all(isinstance(line, string_types) for line in script)
    except:
        return False
