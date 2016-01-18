from glob import glob
from datetime import datetime
import os
import shlex
import subprocess
from subprocess import PIPE

time_fmt = '%Y-%m-%d %H:%M:%S'


extensions = {'trajectories': {'.xtc', '.trr', '.dcd', '.lammpstrj'},
              'topologies': {'.gro', '.pdb'}}


class Task(object):
    def __init__(self, script, project, name=None):
        if name is None:
            name = 'task_{}'.format(project.n_tasks)
        self.name = name
        self.script = script
        self.project = project
        self.current_proc = None

        self.dir = os.path.join(self.project.dir, self.name)
        if not os.path.isdir(self.dir):
            os.mkdir(self.dir)

        for in_file_path in self.project.input_files:
            in_file_name = os.path.split(in_file_path)[1]
            os.symlink(in_file_path, os.path.join(self.dir, in_file_name))

    def execute(self, remote='', credentials=None):
        cwd = os.getcwd()
        os.chdir(self.dir)

        for line in self.script:
            print(datetime.now().strftime(time_fmt), '> Running: ', line)

            args = shlex.split(line)
            proc = subprocess.Popen(args, stdin=PIPE, stdout=PIPE, stderr=PIPE)
            self.current_proc = proc
            out, err = proc.communicate()
            self.log_outputs(out, err)

            print(datetime.now().strftime(time_fmt), '> Success! ')

        os.chdir(cwd)

    def log_outputs(self, out, err):
        stdout_path = os.path.join(self.project.dir, self.name + '_stdout.txt')
        stderr_path = os.path.join(self.project.dir, self.name + '_stderr.txt')
        with open(stdout_path, 'ab') as stdout, open(stderr_path, 'ab') as stderr:
            stdout.write(out)
            stderr.write(err)

    def get_output(self, file_type):
        all_files = list()
        for ext in extensions[file_type]:
            files = glob(os.path.join(self.project.dir, '*{}'.format(ext)))
            all_files.extend(files)
        return all_files

    def status(self):
        pass
