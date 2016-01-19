import shlex
from subprocess import Popen, PIPE


def cmd_line(line):
    """ """
    args = shlex.split(line)
    proc = Popen(args, stdin=PIPE, stdout=PIPE, stderr=PIPE)
    out, err = proc.communicate()
    if 'GROMACS' in err.decode('utf-8'):
        out += err
        err = b''
    return out, err

