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
    def __init__(self, script, project, input_dir=None, name=None):
        self.name = name
        self.script = script
        self.project = project
        self.current_proc = None

        # Copy the input files into the project dir.
        # if input_dir is None:
        #     input_dir = os.
        self.input_dir = os.path.abspath(input_dir)

    def execute(self, remote='', credentials=None):
        # TODO: proper folder management, potential copying of files
        os.chdir(self.input_dir)

        for line in self.script:
            print(datetime.now().strftime(time_fmt), '> Running: ', line)

            args = shlex.split(line)
            proc = subprocess.Popen(args, stdin=PIPE, stdout=PIPE, stderr=PIPE)
            self.current_proc = proc
            out, err = proc.communicate()
            self.log_outputs(out, err)

            print(datetime.now().strftime(time_fmt), '> Success! ')

        #os.chdir(self.project.dir)

    def log_outputs(self, out, err):
        stdout_path = os.path.join(self.project.dir, self.name + '_stdout.txt')
        stderr_path = os.path.join(self.project.dir, self.name + '_stderr.txt')
        with open(stdout_path, 'ab') as stdout, open(stderr_path, 'ab') as stderr:
            stdout.write(out)
            stderr.write(err)

    def get_output(self, file_type):
        all_files = list()
        for ext in extensions[file_type]:
            files = glob(os.path.join(self.input_dir, '*{}'.format(ext)))
            all_files.extend(files)
        return all_files

    def status(self):
        pass
