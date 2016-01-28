import shlex
from subprocess import Popen, PIPE


def cmd_line(line):
    """ """
    args = shlex.split(line)
    proc = Popen(args, stdin=PIPE, stdout=PIPE, stderr=PIPE)
    out, err = proc.communicate()
    # Gromacs logs a lot of its informational output to stderr.
    if 'GROMACS' in err.decode('utf-8'):
        out += err
        err = b''
    return out, err


# TODO: tidy up this madness
def rsync_to(flags, src, dst, user, host, logger=None):
    cmd = 'rsync {flags} {src} {user}@{host}:{dst}'.format(**locals())
    _rsync(cmd, logger)


def rsync_from(flags, src, dst, user, host, logger=None):
    cmd = 'rsync {flags} {user}@{host}:{src} {dst}'.format(**locals())
    _rsync(cmd, logger)


def _rsync(cmd, logger=None):
    out, err = cmd_line(cmd)
    if err:
        raise IOError(err.decode('utf-8'))
    if logger:
        logger.debug(cmd)
        for line in out.splitlines():
            logger.debug(line)
