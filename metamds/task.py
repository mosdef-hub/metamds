import os
from datetime import datetime
import shlex
import subprocess
from subprocess import PIPE


class Task(object):
    def __init__(self, script, project, input_dir=None, name=None):
        self.name = name
        self.script = script
        self.project = project

        # Copy the input files into the project dir.
        # if input_dir is None:
        #     input_dir = os.
        self.input_dir = os.path.abspath(input_dir)

    def execute(self, remote='', credentials=None):
        os.chdir(self.input_dir)
        for line in self.script:
            print(datetime.now(), '> Running: ', line)
            args = shlex.split(line)
            proc = subprocess.Popen(args, stdin=PIPE, stdout=PIPE, stderr=PIPE)
            out, err = proc.communicate()
            self.log_outputs(out, err)
            print(datetime.now(), '> Success! ')
        os.chdir(self.project.dir)

    def log_outputs(self, out, err):
        stdout_path = os.path.join(self.project.dir, self.name + '_stdout.txt')
        stderr_path = os.path.join(self.project.dir, self.name + '_stderr.txt')
        with open(stdout_path, 'ab') as stdout, open(stderr_path, 'ab') as stderr:
            stdout.write(out)
            stderr.write(err)

class Job(object):
    pass
