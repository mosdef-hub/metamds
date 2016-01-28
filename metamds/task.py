from glob import glob
import os

from paramiko import SSHClient

from metamds.io import cmd_line, rsync_from

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
        self.output_dir = os.path.join(self.simulation.output_dir, self.name)
        if not os.path.isdir(self.output_dir):
            os.mkdir(self.output_dir)

        self.hostname = None
        self.username = None
        self.pbs_server = None

    def create_dir(self):
        """Set up the local directory for this task. """
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

    def execute(self, hostname=None, username=None):
        """Execute the task.

        Parameters
        ----------
        hostname : str, optional, default=''
            Execute the task on this remote host.
        username : str, optional, default=''
            Use this username to access `hostname` when executing remotely.

        """
        if hostname:
            self.client = SSHClient()
            self.client.load_system_host_keys()
            self.client.connect(hostname=hostname, username=username)
            # TODO: Is there really not a way to get this from SSHClient()?
            self.client.hostname = hostname
            self.client.username = username

            self.simulation.create_remote_dir(self.client)
            self._execute_remote(self.client)
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
        self.pbs_server = True
        sftp = client.open_sftp()
        pbs_filename = os.path.join(self.simulation.remote_dir, '{}.pbs'.format(self.name))
        with sftp.open(pbs_filename, 'w') as fh:
            header = PBS_HEADER.format(walltime=walltime, name=self.name,
                                       output=os.path.basename(self.simulation.output_dir),
                                       tmp_dir=self.simulation.remote_dir)
            body = '\n'.join(self.script)
            fh.write(''.join((header, body)))

        _, stdout, stderr = client.exec_command('qsub {}'.format(pbs_filename))
        self.pbs_id = stdout.readlines()[0].split('.')[0]

    def _execute_local(self):
        """Execute the task locally. """

        cwd = os.getcwd()
        os.chdir(self.output_dir)

        print(cwd, self.output_dir)
        for line in self.script:
            print(line)
            self.simulation.info.info('Running: {}'.format(line))
            out, err = cmd_line(line)
            for line in out.decode('utf-8').splitlines():
                self.simulation.debug.debug(line)
            for line in err.decode('utf-8').splitlines():
                self.simulation.info.fatal(line)
            self.simulation.info.info('Success!')

        os.chdir(cwd)

    def sync(self):
        if self.simulation.remote_dir:
            out_dir = os.path.split(self.simulation.output_dir)[1]
            rsync_from(flags='-r -h --progress',
                       src=os.path.join(self.simulation.remote_dir, out_dir, self.name, '*'),
                       dst=self.output_dir,
                       user=self.client.username,
                       host=self.client.hostname,
                       logger=self.simulation.debug)
        else:
            print('Nothing to sync.')

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
                files = glob(os.path.join(self.output_dir, '*{}'.format(ext)))
                all_files.extend(files)
        elif file_type.startswith('.'):
            files = glob(os.path.join(self.output_dir, '*{}'.format(file_type)))
            all_files.extend(files)
        return all_files

    def status(self):
        """

        Returns
        -------

        """
        status = dict()
        if self.pbs_server:
            if not self.pbs_id:
                raise RuntimeError('This task does not have a `pbs_id`. Have you'
                                   'run `task.execute` yet?')
            _, stdout, stderr = self.client.exec_command('qstat -f {}'.format(self.pbs_id))
            if stderr.readlines():
                raise IOError(stderr.read().decode('utf-8'))
            content = stdout.readlines()[1:]
            for line in content:
                if '=' in line:
                    entry, value = line.split('=', 1)
                    status[entry.strip()] = value.strip()
        return status




