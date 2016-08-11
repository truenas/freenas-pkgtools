import logging
import logging.config
import math
import syslog
import sys
import select
import subprocess

# To use this:
# from . import Avatar
# os_type = Avatar()

# We may want to have more
# platform-specific stuff.

# Sef likes this line a lot.
_os_type = "FreeNAS"
UPDATE_SERVER = "http://update.ixsystems.com/" + _os_type
MASTER_UPDATE_SERVER = "http://update-master.ixsystems.com/" + _os_type

# For signature verification
IX_CRL = "http://update-master.ixsystems.com/updates/ix_crl.pem"
DEFAULT_CA_FILE = "/usr/local/share/certs/ca-root-nss.crt"
IX_ROOT_CA_FILE = "/usr/local/share/certs/iX-CA.pem"
UPDATE_CERT_DIR = "/usr/local/share/certs"
UPDATE_CERT_PRODUCTION = UPDATE_CERT_DIR + "/Production.pem"
UPDATE_CERT_NIGHTLIES = UPDATE_CERT_DIR + "/Nightlies.pem"
VERIFIER_HELPER = "/usr/local/libexec/verify_signature"
SIGNATURE_FAILURE = True

# TODO: Add FN10's equivalent of get_sw_name (for TN10 when applicable)
try:
    sys.path.append("/usr/local/www")
    from freenasUI.common.system import get_sw_name
    _os_type = get_sw_name()
    UPDATE_SERVER = "http://update.ixsystems.com/" + _os_type
    MASTER_UPDATE_SERVER = "http://update-master.ixsystems.com/" + _os_type
except:
    pass


def Avatar():
    return _os_type


def modified_call(popenargs, logger, **kwargs):
    """
    Variant of subprocess.call that accepts a logger instead of stdout/stderr,
    and logs stdout messages via logger.debug and stderr messages via
    logger.error.

    Original code taken from: https://gist.github.com/hangtwenty/6390750 and modified
    """
    preexec_fn = kwargs.get('preexec_fn')
    stdout_log_level = kwargs.get('stdout_log_level', logging.DEBUG)
    stderr_log_level = kwargs.get('stderr_log_level', logging.ERROR)
    env = kwargs.get('env')
    proc = subprocess.Popen(
        popenargs, preexec_fn=preexec_fn, env=env, stdout=subprocess.PIPE, stderr=subprocess.PIPE
    )

    try:
        log_level = {
            proc.stdout: stdout_log_level,
            proc.stderr: stderr_log_level
        }

        def check_io():
            ready_to_read = select.select([proc.stdout, proc.stderr], [], [], 1000)[0]
            for io in ready_to_read:
                text = io.read().decode('utf8')
                for i in filter(lambda x: x and not x.isspace(), text.split('\n')):
                    logger.log(log_level[io], i)

        # keep checking stdout/stderr until the proc exits
        while proc.poll() is None:
            check_io()

        check_io()  # check again to catch anything after the process exits

        return proc.wait()
    finally:
        proc.stdout.close()
        proc.stderr.close()


class SysLogHandler(logging.Handler):

    priority_names = {
        "alert": syslog.LOG_ALERT,
        "crit": syslog.LOG_CRIT,
        "critical": syslog.LOG_CRIT,
        "debug": syslog.LOG_DEBUG,
        "emerg": syslog.LOG_EMERG,
        "err": syslog.LOG_ERR,
        "error": syslog.LOG_ERR,
        "info": syslog.LOG_INFO,
        "notice": syslog.LOG_NOTICE,
        "panic": syslog.LOG_EMERG,
        "warn": syslog.LOG_WARNING,
        "warning": syslog.LOG_WARNING,
    }

    def __init__(self, facility=syslog.LOG_USER):
        self.facility = facility
        super(SysLogHandler, self).__init__()

    def emit(self, record):
        """
        syslog has a character limit per message
        split the message in chuncks

        The value of 950 is a guess based on tests,
        it could be a little higher.
        """
        syslog.openlog(facility=self.facility)
        msg = self.format(record)
        num_msgs = int(math.ceil(len(msg) / 950.0))
        for i in range(num_msgs):
            if num_msgs == i - 1:
                _msg = msg[950 * i:]
            else:
                _msg = msg[950 * i:950 * (i + 1)]
            syslog.syslog(
                self.priority_names.get(record.levelname.lower(), "debug"),
                _msg)
        syslog.closelog()


class StartsWithFilter(logging.Filter):
    def __init__(self, params):
        self.params = params

    def filter(self, record):
        if self.params:
            allow = not any(record.msg.startswith(x) for x in self.params)
        else:
            allow = True
        return allow

test_logger = logging.getLogger(__name__)

log_config_dict = {
    'version': 1,
    'disable_existing_loggers': False,
    'formatters': {
        'simple': {
            'format': '[%(name)s:%(lineno)s] %(message)s',
        },
    },
    'filters': {
        'cleandownload': {
            '()': StartsWithFilter,
            'params': ['TryGetNetworkFile', 'Searching']
        }
    },
    'handlers': {
        'std': {
            'class': 'logging.StreamHandler',
            'level': 'DEBUG',
            'stream': 'ext://sys.stderr',
            'formatter': 'simple'
        },
        'syslog': {
            'level': 'DEBUG',
            'class': 'freenasOS.SysLogHandler',
            'formatter': 'simple'
        }
    }
}

if not test_logger.hasHandlers():
    log_config_dict['loggers'] = {
        '': {
            'handlers': ['syslog'],
            'level': 'DEBUG',
            'propagate': True
        }
    }


def disable_trygetfilelogs():
    log_config_dict['handlers']['syslog']['filters'] = ['cleandownload']
    logging.config.dictConfig(log_config_dict)


def log_to_stderr():
    log_config_dict['loggers']['']['handlers'] = ['std']
    logging.config.dictConfig(log_config_dict)

logging.config.dictConfig(log_config_dict)
