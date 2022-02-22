from __future__ import print_function
import os
import json
import logging
import re

from . import Exceptions, Package

log = logging.getLogger('freenasOS.Manifest')

# Kinds of validation programs
VALIDATE_UPDATE = "ValidateUpdate"
VALIDATE_INSTALL = "ValidateInstall"

# Where validation programs go (on the update server)
VALIDATION_DIR = "Validators"

SYSTEM_MANIFEST_FILE = "/data/manifest"

# The keys are as follows:
# SEQUENCE_KEY:  A string, uniquely identifying this manifest.
# PACKAGES_KEY:  An array of dictionaries.  They are installed in this order.
# SIGNATURE_KEY:  A string for the signed value of the manifest.
# NOTES_KEY:  An array of name, URL pairs.  Typical names are "README" and "Release Notes".
# TRAIN_KEY:  A string identifying the train for this maifest.
# VERSION_KEY: A string, the friendly name for this particular release. Does not need to be unqiue.
# TIMESTAMP_KEY:	An integer, being the unix time of the build.
# SCHEME_KEY:  A string, identifying the layout version.  Only one value for now.
# NOTICE_KEY:  A string, identifying a message to be displayed before installing this manifest.
# 	This is mainly intended to be used to indicate a particular train is ended.
# A notice is something more important than a release note, and is included in
# the manifest, rather than relying on a URL.
# SWITCH_KEY:  A string, identifying the train that should be used instead.
# This will cause Configuraiton.FindLatestManifest() to use that value instead, so
# it should only be used when a particular train is end-of-life'd.
# REBOOT_KEY:  A boolean, indicaating whether a reboot should be done or not.
# This should RARELY be used, as it will over-ride the Package settings,
# which are a better way to determine rebootability.
# VALIDATE_UPDATE_KEY: A dictionary, specifying a validation program to run for updates.
# VALIDATE_INSTALL_EKY:  A dictionary, specifying a validation program to run for installs.

SEQUENCE_KEY = "Sequence"
PACKAGES_KEY = "Packages"
SIGNATURE_KEY = "Signature"
NOTES_KEY = "Notes"
TRAIN_KEY = "Train"
VERSION_KEY = "Version"
TIMESTAMP_KEY = "BuildTime"
SCHEME_KEY = "Scheme"
NOTICE_KEY = "Notice"
SWITCH_KEY = "NewTrainName"
REBOOT_KEY = "Reboot"
VALIDATE_UPDATE_KEY = "UpdateCheckProgram"
VALIDATE_INSTALL_KEY = "InstallCheckProrgam"

# SCHEME_V1 is the first scheme for packaging and manifests.
# Manifest is at <location>/FreeNAS/<train_name>/LATEST,
# packages are at <location>/Packages

SCHEME_V1 = "version1"


def VerificationCertificateFile(manifest):
    from . import UPDATE_CERT_PRODUCTION, UPDATE_CERT_NIGHTLIES, UPDATE_CERT_DIR

    if manifest is None:
        raise ValueError("Argument cannot be none")

    train = manifest.Train()
    if train is None:
        return UPDATE_CERT_NIGHTLIES

    train_cert = os.path.join(UPDATE_CERT_DIR, train + ".pem")
    if os.path.exists(train_cert):
        return train_cert

    if "STABLE" in train:
        return UPDATE_CERT_PRODUCTION

    return UPDATE_CERT_NIGHTLIES

class ChecksumFailException(Exception):
    pass


def MakeString(obj):
    retval = json.dumps(obj, sort_keys=True, indent=4, separators=(',', ': '), cls=ManifestEncoder)
    return retval


def DiffManifests(m1, m2):
    """
    Compare two manifests.  The return value is a dictionary,
    with at least the following keys/values as possible:
    Packages -- an array of tuples (pkg, op, old)
    Sequence -- a tuple of (old, new)
    Train -- a tuple of (old, new)
    Reboot -- a boolean indicating whether a reboot is necessary.
    (N.B.  This may be speculative; it's going to assume that any
    updates listed in the packages are available, when they may not
    be.)
    If a key is not present, then there are no differences for that
    value.
    """
    return_diffs = {}

    def DiffPackages(old_packages, new_packages):
        retval = []
        old_list = {}
        for P in old_packages:
            old_list[P.Name()] = P

        for P in new_packages:
            if P.Name() in old_list:
                # Either it's the same version, or a new version
                if old_list[P.Name()].Version() != P.Version():
                    retval.append((P, "upgrade", old_list[P.Name()]))
                old_list.pop(P.Name())
            else:
                retval.append((P, "install", None))

        for P in old_list.values():
            retval.insert(0, (P, "delete", None))

        return retval

    # First thing, let's compare the packages
    # This will go into the Packages key, if it's non-empty.
    package_diffs = DiffPackages(m1.Packages(), m2.Packages())
    if len(package_diffs) > 0:
        return_diffs["Packages"] = package_diffs
        # Now let's see if we need to do a reboot
        reboot_required = False
        for pkg, op, old in package_diffs:
            if op == "delete":
                # XXX You know, I hadn't thought this one out.
                # Is there a case where a removal requires a reboot?
                continue
            elif op == "install":
                if pkg.RequiresReboot() == True:
                    reboot_required = True
            elif op == "upgrade":
                # This is a bit trickier.  We want to see
                # if there is an upgrade for old
                upd = pkg.Update(old.Version())
                if upd:
                    if upd.RequiresReboot() == True:
                        reboot_required = True
                else:
                    if pkg.RequiresReboot() == True:
                        reboot_required = True
        return_diffs["Reboot"] = reboot_required

    # Next, let's look at the train
    # XXX If NewTrain is set, should we use that?
    if m1.Train() != m2.Train():
        return_diffs["Train"] = (m1.Train(), m2.Train())

    # Sequence
    if m1.Sequence() != m2.Sequence():
        return_diffs["Sequence"] = (m1.Sequence(), m2.Sequence())

    return return_diffs


def CompareManifests(m1, m2):
    """
    Compare two manifests.  The return value is an
    array of tuples; each tuple is (package, op, old).
    op is "delete", "upgrade", or "install"; for "upgrade",
    the third element of the tuple will be the old version.
    Deleted packages will always be first.
    It assumes m1 is the older, and m2 is the newer.
    This only compares packages; it does not compare
    sequence, train names, notices, etc.
    """
    diffs = DiffManifests(m1, m2)
    if "Packages" in diffs:
        return diffs["Packages"]
    return []


class ManifestEncoder(json.JSONEncoder):

    def default(self, obj):
        if isinstance(obj, Package.Package):
            return obj.dict()
        elif isinstance(obj, Manifest):
            return obj.dict()
        else:
            return json.JSONEncoder.default(self, obj)


class Manifest(object):
    _config = None
    _root = None

    _notes = None
    _train = None
    _packages = None
    _signature = None
    _version = None
    _scheme = SCHEME_V1
    _notice = None
    _switch = None
    _timestamp = None
    _requireSignature = False

    def __init__(self, configuration=None, require_signature=False):
        if configuration is None:
            from . import Configuration
            self._config = Configuration.SystemConfiguration()
        else:
            self._config = configuration
        self._requireSignature = require_signature
        self._dict = {}
        return

    def dict(self):
        return self._dict

    def String(self):
        retval = MakeString(self.dict())
        return retval

    def LoadFile(self, file):
        # Load a manifest from a file-like object.
        # It's loaded as a json file, and then parsed
        if 'b' in file.mode:
            self._dict = json.loads(file.read().decode('utf8'))
        else:
            self._dict = json.loads(file.read())

        self.Validate()
        return

    def LoadPath(self, path):
        # Load a manifest from a path.
        with open(path, "rb") as f:
            self.LoadFile(f)
        return

    def StoreFile(self, f):
        f.write(self.String().encode('utf8'))

    def StorePath(self, path):
        with open(path, "wb") as f:
            self.StoreFile(f)
        return

    def Save(self, root):
        # Need to write out the manifest
        if root is None:
            root = self._root

        if root is None:
            prefix = ""
        else:
            prefix = root
        self.StorePath(prefix + SYSTEM_MANIFEST_FILE)

    def Validate(self):
        # A manifest needs to have a sequence number, train,
        # and some number of packages.  If there is a signature,
        # it needs to match the computed signature.
        from . import SIGNATURE_FAILURE
        if SEQUENCE_KEY not in self._dict:
            raise Exceptions.ManifestInvalidException("Sequence is not set")
        if TRAIN_KEY not in self._dict:
            raise Exceptions.ManifestInvalidException("Train is not set")
        if PACKAGES_KEY not in self._dict \
           or len(self._dict[PACKAGES_KEY]) == 0:
            raise Exceptions.ManifestInvalidException("No packages")
        if self._config and self._config.UpdateServerSigned() == False:
            log.debug("Update server %s [%s] does not sign, so not checking" %
                      (self._config.UpdateServerName(),
                       self._config.UpdateServerURL()))
            return True
        if SIGNATURE_KEY not in self._dict:
            # If we don't have a signature, but one is required,
            # raise an exception
            if self._requireSignature and SIGNATURE_FAILURE:
                log.debug("No signature in manifest")
        else:
            if self._requireSignature:
                if not self.VerifySignature():
                    if self._requireSignature and SIGNATURE_FAILURE:
                        raise Exceptions.ManifestInvalidSignature("Signature verification failed")
                    if not self._requireSignature:
                        log.debug("Ignoring invalid signature due to manifest option")
                    elif not SIGNATURE_FAILURE:
                        log.debug("Ignoring invalid signature due to global configuration")
        return True

    def Notice(self):
        if NOTICE_KEY not in self._dict:
            if (SWITCH_KEY in self._dict):
                # If there's no notice, but there is a train-switch directive,
                # then make up a notice about it.
                return "This train (%s) should no longer be used; please switch to train %s instead" % (self.Train(), self.NewTrain())
            else:
                return None
        else:
            return self._dict[NOTICE_KEY]

    def SetNotice(self, n):
        self._dict[NOTICE_KEY] = n
        if n is None:
            self._dict.pop(NOTICE_KEY)
        return

    def Scheme(self):
        if SCHEME_KEY in self._dict:
            return self._dict[SCHEME_KEY]
        else:
            return None

    def SetScheme(self, s):
        self._dict[SCHEME_KEY] = s
        return

    def Sequence(self):
        return self._dict[SEQUENCE_KEY]

    def SetSequence(self, seq):
        self._dict[SEQUENCE_KEY] = seq
        return

    def SetNote(self, name, location):
        if NOTES_KEY not in self._dict:
            self._dict[NOTES_KEY] = {}
        if location.startswith(self._config.UpdateServerURL()):
            location = location[len(location):]
        self._dict[NOTES_KEY][name] = location

    def Notes(self, raw=False):
        if NOTES_KEY in self._dict:
            rv = {}
            for name in list(self._dict[NOTES_KEY].keys()):
                loc = self._dict[NOTES_KEY][name]
                if raw is False and not loc.startswith(self._config.UpdateServerURL()):
                    loc = "%s/%s/Notes/%s" % (self._config.UpdateServerURL(), self.Train(), loc)
                rv[name] = loc
            return rv
        return None

    def SetNotes(self, notes):
        self._dict[NOTES_KEY] = {}
        if notes is None:
            self._dict.pop(NOTES_KEY)
        else:
            for name, loc in notes.items():
                if loc.startswith(self._config.UpdateServerURL()):
                    loc = loc[len(self._config.UpdateServerURL()):]
                self._dict[NOTES_KEY][name] = os.path.basename(loc)
        return

    def Note(self, name):
        if NOTES_KEY not in self._dict:
            return None
        notes = self._dict[NOTES_KEY]
        if name not in notes:
            return None
        loc = notes[name]
        if not loc.startswith(self._config.UpdateServerURL()):
            loc = self._config.UpdateServerURL + loc
        return loc

    def Train(self):
        if TRAIN_KEY not in self._dict:
            raise Exceptions.ManifestInvalidException("Invalid train")
        return self._dict[TRAIN_KEY]

    def SetTrain(self, train):
        self._dict[TRAIN_KEY] = train
        return

    def Packages(self):
        pkgs = []
        for p in self._dict[PACKAGES_KEY]:
            pkg = Package.Package(p)
            pkgs.append(pkg)
        return pkgs

    def AddPackage(self, pkg):
        if PACKAGES_KEY not in self._dict:
            self._dict[PACKAGES_KEY] = []
        self._dict[PACKAGES_KEY].append(pkg.dict())
        return

    def AddPackages(self, list):
        if PACKAGES_KEY not in self._dict:
            self._dict[PACKAGES_KEY] = []
        for p in list:
            self.AddPackage(p)
        return

    def SetPackages(self, list):
        self._dict[PACKAGES_KEY] = []
        self.AddPackages(list)
        return

    def VerifySignature(self):
        from . import IX_ROOT_CA_FILE, VERIFIER_HELPER, IX_CRL
        from . import SIGNATURE_FAILURE

        if self.Signature() is None:
            return not SIGNATURE_FAILURE
        # Probably need a way to ignore the signature
        else:
            import subprocess
            import tempfile
            from base64 import b64decode
            import OpenSSL.crypto as Crypto
            try:
                cert_file = VerificationCertificateFile(self)
            except ValueError:
                cert_file = None
                
            if not os.path.isfile(IX_ROOT_CA_FILE) \
               or cert_file is None \
               or not os.path.isfile(cert_file):
                log.debug("VerifySignature:  Cannot find a required file")
                return False

            # First we create a store
            store = Crypto.X509Store()
            store.set_flags(Crypto.X509StoreFlags.CRL_CHECK)
            # Load our root CA
            try:
                with open(IX_ROOT_CA_FILE, "r") as f:
                    root_ca = Crypto.load_certificate(Crypto.FILETYPE_PEM, f.read())
                    store.add_cert(root_ca)
            except:
                log.debug("VerifySignature:  Could not load iX root CA", exc_info=True)
                return False
                
            # Now need to get the CRL
            crl_file = tempfile.NamedTemporaryFile(suffix=".pem")
            if crl_file is None:
                log.debug("Could not create CRL, ignoring for now")
            else:
                try:
                    if not self._config.TryGetNetworkFile(
                            url=IX_CRL,
                            pathname=crl_file.name,
                            reason="FetchCRL"
                    ):
                        # TGNF will raise an exception in most cases.
                        raise Exception("Could not get CRL file")
                except:
                    log.error("Could not get CRL file %s" % IX_CRL)
                    crl_file.close()
                    crl_file = None

            if crl_file:
                try:
                    crl = Crypto.load_crl(Crypto.FILETYPE_PEM, crl_file.read())
                    store.add_crl(crl)
                except:
                    log.debug("Could not load CRL, ignoring for now", exc_info=True)
                
            # Now load the certificate files
            try:
                with open(cert_file, "r") as f:
                    regexp = r'-----BEGIN CERTIFICATE-----.*?-----END CERTIFICATE-----'
                    certs = re.findall(regexp, f.read(), re.DOTALL)
            except:
                log.error("Could not load certificates", exc_info=True)
                return false
                    
            # Almost done:  we need the signature as binary data
            try:
                signature = b64decode(self.Signature())
            except:
                log.error("Could not decode signature", exc_info=True)
                return False
            
            verified = False
            tdata = self.dict().copy()
            tdata.pop(SIGNATURE_KEY, None)
            canonical = MakeString(tdata)
            
            for cert in certs:
                try:
                    test_cert = Crypto.load_certificate(Crypto.FILETYPE_PEM, cert)
                    Crypto.verify(test_cert, signature, canonical, "sha256")
                    verified = True
                    break
                except:
                    # For now, just ignore
                    pass

            return verified
        return False

    def Signature(self):
        if SIGNATURE_KEY in self._dict:
            return self._dict[SIGNATURE_KEY]

    def SetSignature(self, signed_hash):
        self._dict[SIGNATURE_KEY] = signed_hash
        return

    def SignWithKey(self, key_data):
        if key_data is None:
            # We'll cheat, and say this means "get rid of the signature"
            if SIGNATURE_KEY in self._dict:
                self._dict.pop(SIGNATURE_KEY)
        else:
            import OpenSSL.crypto as Crypto
            from base64 import b64encode as base64

            # If it's a PKey, we don't need to load it
            if isinstance(key_data, Crypto.PKey):
                key = key_data
            else:
                # Load the key.  This is most likely to fail.
                key = Crypto.load_privatekey(Crypto.FILETYPE_PEM, key_data)

            # Generate a canonical representation of the manifest
            temp = self.dict()
            temp.pop(SIGNATURE_KEY, None)
            tstr = MakeString(temp)

            # Sign it.
            signed_value = base64(Crypto.sign(key, tstr, "sha256"))

            # And now set the signature
            self._dict[SIGNATURE_KEY] = signed_value
        return

    def Version(self):
        if VERSION_KEY in self._dict:
            return self._dict[VERSION_KEY]
        else:
            return None

    def SetVersion(self, version):
        self._dict[VERSION_KEY] = version
        return

    def SetTimeStamp(self, ts):
        self._dict[TIMESTAMP_KEY] = ts

    def TimeStamp(self):
        if TIMESTAMP_KEY in self._dict:
            return self._dict[TIMESTAMP_KEY]
            return 0

    def NewTrain(self):
        if SWITCH_KEY in self._dict:
            return self._dict[SWITCH_KEY]
        else:
            return None

    def SetReboot(self, reboot):
        self._dict[REBOOT_KEY] = reboot
        if reboot is None:
            self._dict.pop(REBOOT_KEY)

    def Reboot(self):
        if REBOOT_KEY in self._dict:
            return self._dict[REBOOT_KEY]
        return None

    def ValidationProgramList(self):
        t = self.ValidationProgram(kind=None)
        if t:
            for kind in t:
                yield t[kind]
    
    def ValidationProgram(self, kind=VALIDATE_UPDATE):
        if kind is None:
            rv = {}
            for k in [VALIDATE_INSTALL_KEY, VALIDATE_UPDATE_KEY]:
                if k in self._dict:
                    rv[k] = self._dict[k]
            return rv
        if kind != VALIDATE_UPDATE:
            log.debug("Invalid validation program kind %s" % str(kind))
            return None
        
        if kind == VALIDATE_UPDATE and VALIDATE_UPDATE_KEY in self._dict:
            return self._dict[VALIDATE_UPDATE_KEY]
        if kind == VALIDATE_INSTALL and VALIDATE_INSTALL_KEY in self._dict:
            return self._dict[VALIDATE_INSTALL_KEY]
        return None

    def AddValidationProgram(self, name, checksum, kind=VALIDATE_UPDATE):
        """
        Add the filename as the validation program.
        Only the last component of the path is used.
        The checksum is generated from the file.
        """
        if kind != VALIDATE_UPDATE:
            raise ValueError("Invalid validation program kind %s" % str(kind))
        if kind == VALIDATE_UPDATE:
            key = VALIDATE_UPDATE_KEY
        elif kind == VALIDATE_INSTALL:
            key = VALIDATE_INSTALL_KEY
        else:
            raise ValueError("Unknown validation kind %s" % str(kind))
        vdict = {}
        self._dict[key] = vdict
        if name is None:
            # Similar to methods above, None means to remove the element
            self._dict.pop(key)
            return
        vdict["Name"] = name
        vdict["Checksum"] = checksum
        vdict["Kind"] = kind
        return

    def RunValidationProgram(self, cache_dir, kind=VALIDATE_UPDATE):
        # Not sure this should go here
        # kind is currently unused.
        import subprocess, hashlib, tempfile

        old_version = self._config.SystemManifest().Version()
        old_sequence = self._config.SystemManifest().Sequence()
        new_version = self.Version()
        new_sequence = self.Sequence()
        valid_dir = "."
        valid_script = "."
        
        def PreExecHook():
            import pwd
            try:
                uid = pwd.getpwnam("nobody").pw_uid
            except:
                # This is what freebsd uses for nobody
                uid = 65534
            os.environ["CURRENT_VERSION"] = old_version
            os.environ["CURRENT_SEQUENCE"] = old_sequence
            os.environ["NEW_VERSION"] = new_version
            os.environ["NEW_SEQUENCE"] = new_sequence
            os.chdir(valid_dir)
            os.setegid(uid)
            os.seteuid(uid)
            
        if kind != VALIDATE_UPDATE:
            raise ValueError("Invalid validation program kind %s" % str(kind))
        
        v = self.ValidationProgram(kind)
        if v is None:
            # No validation to run, therefore good
            return True
        tmp_file = None
        if cache_dir is None:
            tmp_file = tempfile.NamedTemporaryFile(delete=False)
            prog_path = tmp_file.name
        else:
            prog_path = os.path.join(cache_dir, kind)
        if tmp_file or (not os.path.exists(prog_path)):
            # If tmp_file is set, we did not have a cache directory,
            # and so it's not possible to have it pre-downloaded.
            # Need to download it
            # This may raise an exception, in which case we let it propagate up
            self._config.TryGetNetworkFile(file="%s/%s" % (VALIDATION_DIR, v["Name"]),
                                           pathname=prog_path,
                                           reason="Validation Script").close()
        with open(prog_path, "rb") as f:
            hash = hashlib.sha256(f.read()).hexdigest()
        if tmp_file:
            tmp_file.close()
        if hash != v["Checksum"]:
            # Let's attempt to remove it, as well
            try:
                os.remove(prog_path)
            except:
                pass
            raise Exceptions.ChecksumFailException("Validation program %s" % v["Name"])
        try:
            os.lchmod(prog_path, 0o555)
        except:
            pass
        valid_dir = os.path.dirname(prog_path)
        valid_script = os.path.join(".", os.path.basename(prog_path))
        
        try:
            subprocess.check_output(valid_script, preexec_fn=PreExecHook, stderr=subprocess.STDOUT, encoding="utf-8",
                                    errors="ignore")
        except subprocess.CalledProcessError as err:
            raise Exceptions.UpdateInvalidUpdateException(err.output.rstrip())
        finally:
            if tmp_file: os.remove(prog_path)
            
        return True
    
