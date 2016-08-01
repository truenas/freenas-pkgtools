import logging
import logging.config
import math
import syslog
import sys

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
