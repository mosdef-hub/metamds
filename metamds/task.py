from glob import glob
from datetime import datetime
import os
import shlex
import subprocess
from subprocess import PIPE
import tarfile

from paramiko import SSHClient


TIME_FMT = '%Y-%m-%d %H:%M:%S'


EXTENSIONS = {'trajectories': {'.xtc', '.trr', '.dcd', '.lammpstrj'},
              'topologies': {'.gro', '.pdb'}}

PBS_HEADER = """#!/bin/sh -l
#PBS -j oe
#PBS -l nodes=1:ppn=16
#PBS -l walltime={walltime}
#PBS -q low
#PBS -N {name}

cd $PBS_O_WORKDIR/{output}/{name}

echo $PWD

module load gromacs

"""


class Task(object):
    def __init__(self, script=None, project=None, name=None):
        if name is None:
            name = 'task_{:d}'.format(project.n_tasks)
        self.name = name
        self.script = script
        self.project = project
        self.current_proc = None
        self._setup_local_dir()

    def execute(self, hostname='', username=''):
        if hostname:
            client = SSHClient()
            client.load_system_host_keys()
            client.connect(hostname='rahman.vuse.vanderbilt.edu', username='ctk3b')
            self._setup_remote_dir(client, hostname, username)
            self._execute_remote(client)
        else:
            self._execute_local()

    def _execute_remote(self, client, walltime='96:00:00'):
        # if uses_PBS(client):
        sftp = client.open_sftp()
        pbs_filename = os.path.join(self.tmp_dir, '{}.pbs'.format(self.name))
        with sftp.open(pbs_filename, 'w') as fh:
            pbs_file = ''.join((PBS_HEADER.format(walltime=walltime, name=self.name, output=os.path.basename(self.project.dir)),
                               '\n'.join(self.script)))
            fh.write(pbs_file)

        stdin, stdout, stderr = client.exec_command('cd {}'.format(self.tmp_dir))
        stdin, stdout, stderr = client.exec_command('qsub {}'.format(pbs_filename))
        print(stdout.readlines())
        print(stderr.readlines())
        import ipdb; ipdb.set_trace()

    def _setup_remote_dir(self, client, host, user):
        stdin, stdout, stderr = client.exec_command('mktemp -d')
        if stderr.readlines():
            raise IOError(stderr.readlines()[0].rstrip())
        else:
            self.tmp_dir = stdout.readlines()[0].rstrip()

        cmd = 'rsync -h --progress --partial {src} {user}@{host}:{dst}'.format(
            src=' '.join(self.project.input_files), dst=self.tmp_dir,
            user=user, host=host)
        self._subprocess_and_log(cmd)

        cmd = 'rsync -r -h --links --progress --partial {src} {user}@{host}:{dst}'.format(
            src=self.project.dir, dst=self.tmp_dir, user=user, host=host)
        self._subprocess_and_log(cmd)

    def _execute_local(self):
        cwd = os.getcwd()
        os.chdir(self.dir)

        for line in self.script:
            print(datetime.now().strftime(TIME_FMT), '> Running: ', line)
            self._subprocess_and_log(line)
            print(datetime.now().strftime(TIME_FMT), '> Success! ')

        os.chdir(cwd)

    def _setup_local_dir(self):
        self.dir = os.path.join(self.project.dir, self.name)
        if not os.path.isdir(self.dir):
            os.mkdir(self.dir)

        for in_file_path in self.project.input_files:
            # TODO: tidy up
            in_file_name = os.path.split(in_file_path)[1]
            link_path = os.path.join(self.dir, in_file_name)

            cwd = os.getcwd()
            os.chdir(self.dir)

            rel_in_path = os.path.relpath(in_file_path, self.dir)
            rel_link_path = os.path.relpath(link_path, self.dir)

            os.symlink(rel_in_path, rel_link_path)
            os.chdir(cwd)

    def _subprocess_and_log(self, line):
        args = shlex.split(line)
        proc = subprocess.Popen(args, stdin=PIPE, stdout=PIPE, stderr=PIPE)
        self.current_proc = proc
        out, err = proc.communicate()
        self.log_outputs(out, err)

    def log_outputs(self, out, err):
        stdout_path = os.path.join(self.project.dir, self.name + '_stdout.txt')
        stderr_path = os.path.join(self.project.dir, self.name + '_stderr.txt')
        with open(stdout_path, 'ab') as stdout, open(stderr_path, 'ab') as stderr:
            stdout.write(out)
            stderr.write(err)

    def get_output(self, file_type):
        all_files = list()
        for ext in EXTENSIONS[file_type]:
            files = glob(os.path.join(self.project.dir, '*{}'.format(ext)))
            all_files.extend(files)
        return all_files

    def status(self):
        pass
