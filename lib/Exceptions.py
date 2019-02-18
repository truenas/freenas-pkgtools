class ConfigurationInvalidException(Exception):
    pass


class ChecksumFailException(Exception):
    pass


class ManifestInvalidException(Exception):
    pass


class ManifestInvalidSignature(Exception):
    pass


class UpdateException(Exception):
    pass

class UpdateNetworkException(UpdateException):
    """
    Base class for a set of exceptions related to networking.
    """
    pass

class UpdateNetworkFileNotFoundException(UpdateNetworkException):
    """
    This is a 404 error.
    """
    pass

class UpdateNetworkServerException(UpdateNetworkException):
    """
    Generic network error.
    """
    pass

class UpdateNetworkConnectionException(UpdateNetworkException):
    """
    Unable to connect to the server.  This covers a lot
    of potential reasons, including a bad name (or nameserver),
    or the server is not running the correct type of service.
    """
    pass

class UpdateBadFrozenFile(Exception):
    """
    Indicates a frozen update file was bad
    """
    pass

class UpdateInsufficientSpace(UpdateException):
    """Raised when there is insufficient space to download
    a file or install into a new BE.
    Attributes:
      value -- a string containing the error message from the script
    """
    def __init__(self, value=""):
        self.value = value
    def __str__(self):
        return repr(self.value)
    
class UpdateInvalidUpdateException(UpdateException):
    """Raised when a package validation script fails.
    Attributes:
       value -- string containing the error message from the script
    """
    def __init__(self, value=""):
        self.value = value
    def __str__(self):
        return repr(self.value)

class UpdateIncompleteCacheException(UpdateException):
    pass


class UpdateInvalidCacheException(UpdateException):
    pass


class UpdateBusyCacheException(UpdateException):
    pass


class UpdatePackageNotFound(UpdateException):
    pass

class UpdateManifestNotFound(UpdateException):
    pass


class UpdateApplyException(UpdateException):
    pass


class InvalidBootEnvironmentNameException(UpdateException):
    pass

class UpdateBootEnvironmentException(UpdateException):
    pass


class UpdateSnapshotException(UpdateException):
    pass


class UpdatePackageException(UpdateException):
    pass
