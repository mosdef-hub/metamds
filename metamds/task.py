from glob import glob
import os

from paramiko import SSHClient

from metamds.io import cmd_line

EXTENSIONS = {'trajectories': {'.xtc', '.trr', '.dcd', '.lammpstrj'},
              'topologies': {'.gro', '.pdb'}}

PBS_HEADER = """#!/bin/sh -l
#PBS -j oe
#PBS -l nodes=1:ppn=16
#PBS -l walltime={walltime}
#PBS -q low
#PBS -N {name}

echo $PWD
cd {tmp_dir}/{output}/{name}
echo $PWD

module load gromacs

"""


class Task(object):
    def __init__(self, script=None, simulation=None, name=None):
        if name is None:
            name = 'task_{:d}'.format(simulation.n_tasks)
        self.name = name
        self.script = script
        self.simulation = simulation
        self.current_proc = None
        self.output_dir = None

    def create_dir(self):
        """Set up the local directory this task. """
        self.output_dir = os.path.join(self.simulation.output_dir, self.name)
        if not os.path.isdir(self.output_dir):
            os.mkdir(self.output_dir)

        for in_file_path in self.simulation.input_files:
            # TODO: tidy up
            in_file_name = os.path.split(in_file_path)[1]
            link_path = os.path.join(self.output_dir, in_file_name)

            cwd = os.getcwd()
            os.chdir(self.output_dir)

            rel_in_path = os.path.relpath(in_file_path, self.output_dir)
            rel_link_path = os.path.relpath(link_path, self.output_dir)

            os.symlink(rel_in_path, rel_link_path)
            os.chdir(cwd)

    def execute(self, hostname='', username=''):
        """Execute the task.

        Parameters
        ----------
        hostname : str, optional, default=''
            Execute the task on this remote host.
        username : str, optional, default=''
            Use this username to access `hostname` when executing remotely.

        """
        if hostname:
            client = SSHClient()
            client.load_system_host_keys()
            client.connect(hostname='rahman.vuse.vanderbilt.edu', username='ctk3b')
            self.simulation.create_remote_dir(client, hostname, username)
            self._execute_remote(client)
        else:
            self._execute_local()

    def _execute_remote(self, client, walltime='1:00:00'):
        """Execute the task on a remote server.

        Parameters
        ----------
        client : paramiko.SSHClient
        walltime : str, optional

        """
        # if uses_PBS(client):
        sftp = client.open_sftp()
        pbs_filename = os.path.join(self.simulation.remote_dir, '{}.pbs'.format(self.name))
        with sftp.open(pbs_filename, 'w') as fh:
            header = PBS_HEADER.format(walltime=walltime, name=self.name,
                                       output=os.path.basename(self.simulation.output_dir),
                                       tmp_dir=self.simulation.remote_dir)
            body = '\n'.join(self.script)
            fh.write(''.join((header, body)))

        stdin, stdout, stderr = client.exec_command('qsub {}'.format(pbs_filename))

    def _execute_local(self):
        """Execute the task locally. """

        cwd = os.getcwd()
        os.chdir(self.output_dir)

        for line in self.script:
            self.simulation.info.info('Running: {}'.format(line))
            out, err = cmd_line(line)
            for line in out.decode('utf-8').splitlines():
                self.simulation.debug.debug(line)
            for line in err.decode('utf-8').splitlines():
                self.simulation.info.fatal(line)
            self.simulation.info.info('Success!')

        os.chdir(cwd)

    def get_output_files(self, file_type):
        """Get all files of a specific type produced by this task.

        Parameters
        ----------
        file_type : str
            An extension or a keyword for a category of file types present in
            `EXTENSIONS`.

        """
        all_files = list()
        if file_type in EXTENSIONS:
            for ext in EXTENSIONS[file_type]:
                files = glob(os.path.join(self.simulation.output_dir, '*{}'.format(ext)))
                all_files.extend(files)
        elif file_type.startswith('.'):
            files = glob(os.path.join(self.simulation.output_dir, '*{}'.format(file_type)))
            all_files.extend(files)
        return all_files

    def status(self):
        """

        Returns
        -------

        """
        pass
