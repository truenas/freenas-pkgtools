from __future__ import print_function
from datetime import datetime
import ctypes
import logging
import os
import signal
import subprocess
import sys
import random
import shutil
import fcntl
import errno
import tarfile

try:
    import libzfs
except ImportError:
    # This might happen during an install of freenas
    # as py-libzfs is not available as yet, in which
    # case just pass
    pass

from . import Avatar, modified_call
import freenasOS.Manifest as Manifest
import freenasOS.Configuration as Configuration
import freenasOS.Installer as Installer
from freenasOS.Exceptions import (
    UpdateIncompleteCacheException, UpdateInvalidCacheException, UpdateBusyCacheException,
    UpdateBootEnvironmentException, UpdatePackageException, UpdateSnapshotException,
    ManifestInvalidSignature, UpdateManifestNotFound, UpdateInsufficientSpace,
    InvalidBootEnvironmentNameException, UpdateBadFrozenFile,
)

log = logging.getLogger('freenasOS.Update')

debug = False

REQUIRE_REBOOT = True

# Types of package files to download.
# The options are PkgFileAny (aka None),
# which means to download whatever you can (delta
# being preferred), PkgFileDeltaOnly , which means to
# download only a delta file (and fail if we can't),
# and PkgFileFullOnly, which means to only download the
# full package file
PkgFileAny = None
PkgFileDeltaOnly = "delta-only"
PkgFileFullOnly = "full-only"


SERVICES = {
    "SMB": {
        "Name": "CIFS",
        "ServiceName": "cifs",
        "Description": "Restart CIFS sharing",
        "CheckStatus": True,
    },
    "AFP": {
        "Name": "AFP",
        "ServiceName": "afp",
        "Description": "Restart AFP sharing",
        "CheckStatus": True,
    },
    "NFS": {
        "Name": "NFS",
        "ServiceName": "nfs",
        "Description": "Restart NFS sharing",
        "CheckStatus": True,
    },
    "iSCSI": {
        "Name": "iSCSI",
        "ServiceName": "iscsitarget",
        "Description": "Restart iSCSI services",
        "CheckStatus": True,
    },
    "FTP": {
        "Name": "FTP",
        "ServiceName": "ftp",
        "Description": "Restart FTP services",
        "CheckStatus": True,
    },
    "WebDAV": {
        "Name": "WebDAV",
        "ServiceName": "webdav",
        "Description": "Restart WebDAV services",
        "CheckStatus": True,
    },
    # Not sure what DirectoryServices would be
    #    "DirectoryServices" : {
    #        "Name" : "Restart directory services",
}


def IsFN9():
    """
    This returns whether or not we're running on Free/TrueNAS 9.
    This is necessary because the service-related tasks are
    handled differently in FN9 vs FN10.
    """
    return os.path.exists("/usr/local/www/freenasUI")

if IsFN9():
    SERVICES["WebUI"] = {
        "Name": "WebUI",
        "ServiceName": "django",
        "Description": "Restart Web UI (forces a logout)",
        "CheckStatus": False,
    }
else:
    SERVICES["gui"] = {
        "Name": "gui",
        "ServiceName": "gui",
        "Description": "Restart Web UI (forces a logout)",
        "CheckStatus": False,
    }


def GetServiceDescription(svc):
    if svc not in SERVICES:
        return None
    return SERVICES[svc]["Description"]


def VerifyServices(svc_names):
    """
    Verify whether the requested services are known or not.
    This is a trivial wrapper for now.
    """
    for name in svc_names:
        if name not in SERVICES:
            return False
    return True


def StopServices(svc_list):
    """
    Stop a set of services.  Returns the list of those that
    were stopped.
    """
    retval = []
    if IsFN9():
        old_path = []
        old_environ = None
        if "DJANGO_SETTINGS_MODULE" not in os.environ:
            old_environ = True
            os.environ["DJANGO_SETTINGS_MODULE"] = "freenasUI.settings"
        if "/usr/local/www" not in sys.path:
            old_path.append("/usr/local/www")
            sys.path.append("/usr/local/www")
        if "/usr/local/www/freenasUI" not in sys.path:
            old_path.append("/usr/local/www/freenasUI")
            sys.path.append("/usr/local/www/freenasUI")

        from django.db.models.loading import cache
        cache.get_apps()

        from freenasUI.middleware.notifier import notifier
        n = notifier()
        # Hm, this doesn't handle any particular ordering.
        # May need to fix this.
        for svc in svc_list:
            if svc not in SERVICES:
                raise ValueError("%s is not a known service" % svc)
            s = SERVICES[svc]
            svc_name = s["ServiceName"]
            log.debug("StopServices:  svc %s maps to %s" % (svc, svc_name))
            if (not s["CheckStatus"]) or n.started(svc_name):
                retval.append(svc)
                n.stop(svc_name)
            else:
                log.debug("svc %s is not started" % svc)

        # Should I remove the environment settings?
        if old_environ:
            os.environ.pop("DJANGO_SETTINGS_MODULE")
        for p in old_path:
            sys.path.remove(p)
    else:
    # Hm, this doesn't handle any particular ordering.
    # May need to fix this.
    # TODO: Uncomment the below when freenas10 is ready for rebootless updates
    # But also fix it for being appropriate to freenas10
    # for svc in svc_list:
    #     if not svc in SERVICES:
    #         raise ValueError("%s is not a known service" % svc)
    #     s = SERVICES[svc]
    #     svc_name = s["ServiceName"]
    #     log.debug("StopServices:  svc %s maps to %s" % (svc, svc_name))
    #     if (not s["CheckStatus"]) or n.started(svc_name):
    #         retval.append(svc)
    #         n.stop(svc_name)
    #     else:
    #         log.debug("svc %s is not started" % svc)
        pass
    return retval


def StartServices(svc_list):
    """
    Start a set of services.  THis is the output
    from StopServices
    """
    if IsFN9():
        old_path = []
        old_environ = None
        if "DJANGO_SETTINGS_MODULE" not in os.environ:
            old_environ = True
            os.environ["DJANGO_SETTINGS_MODULE"] = "freenasUI.settings"
        if "/usr/local/www" not in sys.path:
            old_path.append("/usr/local/www")
            sys.path.append("/usr/local/www")
        if "/usr/local/www/freenasUI" not in sys.path:
            old_path.append("/usr/local/www/freenasUI")
            sys.path.append("/usr/local/www/freenasUI")

        from django.db.models.loading import cache
        cache.get_apps()

        from freenasUI.middleware.notifier import notifier
        n = notifier()
        # Hm, this doesn't handle any particular ordering.
        # May need to fix this.
        for svc in svc_list:
            if svc not in SERVICES:
                raise ValueError("%s is not a known service" % svc)
            svc_name = SERVICES[svc]["ServiceName"]
            n.start(svc_name)

        # Should I remove the environment settings?
        if old_environ:
            os.environ.pop("DJANGO_SETTINGS_MODULE")
        for p in old_path:
            sys.path.remove(p)

    else:
        pass
    return


# Used by the clone functions below
beadm = "/usr/local/sbin/beadm"
dsinit = "/usr/local/sbin/dsinit"
freenas_pool = "freenas-boot"

def RunCommand(command, args):
    # Run the given command.  Uses subprocess module.
    # Returns True if the command exited with 0, or
    # False otherwise.

    proc_args = [command]
    if args is not None:
        proc_args.extend(args)
    log.debug("RunCommand(%s, %s)" % (command, args))
    if debug:
        print(proc_args, file=sys.stderr)
        child = 0
    else:
        libc = ctypes.cdll.LoadLibrary("libc.so.7")
        omask = (ctypes.c_uint32 * 4)(0, 0, 0, 0)
        mask = (ctypes.c_uint32 * 4)(0, 0, 0, 0)
        pmask = ctypes.pointer(mask)
        pomask = ctypes.pointer(omask)
        libc.sigprocmask(signal.SIGQUIT, pmask, pomask)
        try:
            child = modified_call(proc_args, log)
        except:
            return False
        libc.sigprocmask(signal.SIGQUIT, pomask, None)

    if child == 0:
        return True
    else:
        return False


def GetRootDataset():
    # Returns the name of the root dataset.
    # This will be of the form zroot/ROOT/<be-name>
    cmd = ["/bin/df", "/"]
    if debug:
        print(cmd, file=sys.stderr)
        return None
    try:
        p = subprocess.Popen(cmd, stdout=subprocess.PIPE)
    except:
        log.error("Could not run %s", cmd)
        return None
    stdout, stderr = p.communicate()
    if p.returncode != 0:
        log.error("%s returned %d" % (cmd, p.returncode))
        return None
    lines = stdout.decode('utf8').rstrip().split("\n")
    if len(lines) != 2:
        log.error("Unexpected output from %s, too many lines (%d):  %s" % (cmd, len(lines), lines))
        return None
    if not lines[0].startswith("Filesystem"):
        log.error("Unexpected output from %s:  %s" % (cmd, lines[0]))
        return None
    rv = lines[1].split()[0]
    return rv


def CloneSetAttr(clone, **kwargs):
    """
    Given a clone, set attributes defined in kwargs.
    Currently only 'keep' (which maps to beadm:keep)
    is allowed.
    """
    if clone is None:
        raise ValueError("Clone must be set")
    if kwargs is None:
        return True

    dsname = "freenas-boot/ROOT/{0}".format(clone["realname"])
    try:
        with libzfs.ZFS() as zfs:
            ds = zfs.get_dataset(dsname)
    except:
        log.debug("Unable to find BE {0}".format(clone["realname"]), exc_info=True)
        return False
    
    for k, v in kwargs.items():
        if k == "keep":
            # This maps to zfs set beadm:keep=%s freenas-boot/ROOT/${bename}
            try:
                with libzfs.ZFS() as zfs:
                    ds = zfs.get_dataset(dsname)
                    if "beadm:keep" in ds.properties:
                        ds.properties["beadm:keep"].value = str(v)
                    else:
                        ds.properties["beadm:keep"] = libzfs.ZFSUserProperty(str(v))
            except:
                log.debug("Unable to set beadm:keep value on BE {0}".format(clone["realname"]), exc_info=True)
                return False
        elif k == "sync":
            try:
                with libzfs.ZFS() as zfs:
                    ds = zfs.get_dataset(dsname)
                    if v is None:
                        ds.properties["sync"].inherit()
                    else:
                        ds.properties["sync"].value = v
            except:
                log.debug("Unable to set dataset sync value on BE {0} to {1}".
                          format(clone["realname"], str(v)), exc_info=True)
                return False
    return True

def PruneClones(cb=None, required=0):
    """
    Attempt to prune boot environments based on age.
    It will try deleting BEs until either:
    1:  There are no more BEs suitable for deletion.
    2:  At least 80% of the pool is free.
    3:  At least 2gbytes is free.
    If cb is not None, it will be called with something.
    required should be the estimated size (in bytes)
    needed for the install.
    """
    def PruneDone(req):
        # We'll say an install requires at least 512mbytes.
        mbytes_min = 512 * 1024 * 1024
        if req > mbytes_min:
            mbytes_min = req
            
        return Configuration.CheckFreeSpace(pool=freenas_pool, required=mbytes_min)


    def DCW(be):
        """
        Dead Clone Walking:  return true if the
        clone is eligible for pruning.  That is, if
        it does not have a keep property set to True,
        and is not currently mounted or active.
        For now, if "keep" is not in it, we exclude it
        as well, but log it.
        """
        if "keep" not in be:
            log.debug(
                "Cannot prune clone {0} since it is missing a keep option".format(be["name"])
            )
            return False
        if be["keep"] is None:
            log.debug("Cannot prune clone {0} since keep is None".format(be["name"]))
            return False
        if be["keep"] == True:
            return False
        if be["mountpoint"] != "-":
            log.debug("Cannot prune clone {0} since it is mounted at {1}".format(be["name"], be["mountpoint"]))
            return False
        if be["active"] != "-":
            log.debug(
                "Cannot prune clone {0} since it is active {1}".format(be["name"], be["active"])
            )
            return False
        return True


    
    if PruneDone(required):
        log.debug("No pruning necessary")
        return True
    clones = sorted(ListClones(), key=lambda be: be["created"])
    for be in clones:
        # Check if clone is eligible.
        if DCW(be):
            log.debug("I want to get rid of clone %s" % be["name"])
            if DeleteClone(be["realname"]) is True:
                log.debug("Successfully deleted clone %s" % be["realname"])
                if PruneDone(required):
                    log.debug("Pruning done!")
                    return True
            else:
                log.debug("Could not delete clone %s" % be["realname"])
        else:
            log.debug("Clone %s not eligible for pruning" % be["realname"])
        # Next BE, please

    log.debug("Done with prune loop.  Must have failed.")
    return False


def ListClones():
    # Return a list of boot-environment clones.
    # The outer loop is just a simple wrapper for
    # "beadm list -H"; it then gets a set of properties
    # for each BE.
    # Because of that, it can't use RunCommand
    cmd = [beadm, "list", "-H"]
    rv = []
    if debug:
        print(cmd, file=sys.stderr)
        return None
    try:
        p = subprocess.Popen(cmd, stdout=subprocess.PIPE)
    except:
        log.error("Could not run %s", cmd)
        return None
    stdout, stderr = p.communicate()
    if p.returncode != 0:
        log.error("`%s' returned %d" % (cmd, p.returncode))
        return None

    for line in stdout.decode('utf8').strip('\n').split('\n'):
        fields = line.split('\t')
        name = fields[0]
        if len(fields) > 5 and fields[5] != "-":
            name = fields[5]
        tdict = {
            'realname': fields[0],
            'name': name,
            'active': fields[1],
            'mountpoint': fields[2],
            'space': fields[3],
            'created': datetime.strptime(fields[4], '%Y-%m-%d %H:%M'),
            'keep': None,
            'rawspace': None
        }
        try:
            with libzfs.ZFS() as zfs:
                ds = zfs.get_dataset("freenas-boot/ROOT/{0}".format(tdict["realname"]))
                tdict["rawspace"] = ds.properties["used"].rawvalue
                try:
                    kstr = ds.properties["beadm:keep"].value
                    if kstr == "True":
                        tdict["keep"] = True
                    elif kstr == "False":
                        tdict["keep"] = False
                except KeyError:
                    pass
        except libzfs.ZFSException:
            pass
        rv.append(tdict)
    return rv


def FindClone(name):
    """
    Find a BE with the given name.  We look for nickname first,
    and then realname.  In order to do this, we first have to
    get the list of clones.
    Returns None if it can't be found, otherwise returns a
    dictionary.
    """
    rv = None
    clones = ListClones()
    for clone in clones:
        if clone["name"] == name:
            rv = clone
            break
        if clone["realname"] == name and rv is None:
            rv = clone
    return rv

"""
Notes to self:
/beadm create pre-${NEW}
zfs snapshot freenas-boot/ROOT/${CURRENT}@Pre-Upgrade-${NEW}
zfs inherit -r beadm:nickname freenas-boot/ROOT/${CURRENT}@Pre-Upgrade-${NEW}
/beadm rename -n ${CURRENT} ${NEW}
/beadm rename -n pre-{$NEW} ${CURRENT}

# Failure
    /beadm destroy -F ${CURRENT}
    /beadm rename -n ${NEW} ${CURRENT}
    zfs rollback freenas-boot/ROOT/${CURRENT}@Pre-Upgrade-${NEW}
    zfs set beadm:nickname=${CURRENT} freenas-boot/ROOT/${CURRENT}
# Success
    /beadm activate ${NEW}	# Not sure that's necessary or will work

# Either case
zfs destroy -r freenas-boot/ROOT/${CURRENT}@Pre-Upgrade-${NEW}
"""


def _CheckBEName(name):
    # Disallow certain characters, because beadm(8) doesn't
    # quote or validate very well.
    badChars = "/ *'\"?@"
    if any(elem in name for elem in badChars):
        raise InvalidBootEnvironmentNameException
    
def CreateClone(name, bename=None, rename=None):
    # Create a boot environment from the current
    # root, using the given name.  Returns False
    # if it could not create it
    # If rename is set, we need to create the clone with
    # a temporary name, rename the root BE to its new
    # name, and then rename the new clone to the root name.
    # See above, excluding the snapshot.
    # If rename is set, then we want to create a new,
    # temporary BE, with the name pre-${name}; then
    # we rename ${rename} to ${name}, and then rename
    # pre-${name} to ${rename}.  In the event of
    # an error anywhere along, we undo as much as we can
    # and return an error.
    args = ["create"]
    if bename:
        _CheckBEName(bename)
        # Due to how beadm works, if we are given a starting name,
        # we need to find the real name.
        cl = FindClone(bename)
        if cl is None:
            log.error("CreateClone:  Cannot find starting clone %s" % bename)
            return False
        log.debug("FindClone returned %s" % cl)
        args.extend(["-e", cl["realname"]])
    if rename:
        _CheckBEName(rename)
        temp_name = "Pre-%s-%d" % (name, random.SystemRandom().randint(0, 1024 * 1024))
        args.append(temp_name)
        log.debug("CreateClone with rename, temp_name = %s" % temp_name)
    else:
        args.append(name)

    # Let's see if the given name already exists
    try:
        with libzfs.ZFS() as zfs:
            zfs.get_dataset("freenas-boot/ROOT/{0}".format(name))
    except libzfs.ZFSException:
        pass
    else:
        raise KeyError
    
    try:
        if os.path.exists(dsinit) and not RunCommand(dsinit, ["--lock"]):
            return False

        rv = RunCommand(beadm, args)
        if rv is False:
            return False
    finally:
        if os.path.exists(dsinit) and not RunCommand(dsinit, ["--unlock"]):
            return False

    if rename:
        # We've created Pre-<newname>-<random>
        # Now we want to reame the root environment, which is rename, to
        # the new name.
        args = ["rename", rename, name]
        rv = RunCommand(beadm, args)
        if rv is False:
            # We failed.  Clean up the temp one
            args = ["destroy", "-F", temp_name]
            RunCommand(beadm, args)
            return False
        # Root has been renamed, so let's rename the temporary one
        args = ["rename", temp_name, rename]
        rv = RunCommand(beadm, args)
        if rv is False:
            # We failed here.  How annoying.
            # So let's delete the newlyp-created BE
            # and rename root
            args = ["destroy", "-F", rename]
            RunCommand(beadm, args)
            args = ["rename", name, rename]
            RunCommand(beadm, args)
            return False

    return True


def RenameClone(oldname, newname):
    # Create a boot environment from the current
    # root, using the given name.  Returns False
    # if it could not create it
    _CheckBEName(oldname)
    _CheckBEName(newname)
    
    args = ["rename", oldname, newname]
    rv = RunCommand(beadm, args)
    if rv is False:
        return False
    return True


def MountClone(name, mountpoint=None):
    # Mount the given boot environment.  It will
    # create a random name in /tmp.  Returns the
    # name of the mountpoint, or None on error.
    if mountpoint is None:
        import tempfile
        try:
            mount_point = tempfile.mkdtemp()
        except:
            return None
    else:
        mount_point = mountpoint

    if mount_point is None:
        return None
    args = ["mount", name, mount_point]
    rv = RunCommand(beadm, args)
    if rv is False:
        try:
            os.rmdir(mount_point)
        except:
            pass
        return None

    # If all that worked... we now need
    # to set up /dev, /var/tmp
    # Let's see if we need to do that
    # Now let's mount devfs, tmpfs
    args_array = [
        ["-t", "devfs", "devfs", mount_point + "/dev"],
        ["-t", "tmpfs", "tmpfs", mount_point + "/var/tmp"],
    ]
    cmd = "/sbin/mount"
    for fs_args in args_array:
        rv = RunCommand(cmd, fs_args)
        if rv is False:
            UnmountClone(name, None)
            return None

    return mount_point


def ActivateClone(name):
    # Set the clone to be active for the next boot
    args = ["activate", name]
    return RunCommand(beadm, args)


def UnmountClone(name, mount_point=None):
    # Unmount the given clone.  After unmounting,
    # it removes the mount directory.
    # We can also unmount /dev and /var/tmp
    # If this fails, we ignore it for now
    if mount_point is not None:
        cmd = "/sbin/umount"
        for dir in ["/dev", "/var/tmp"]:
            args = ["-f", mount_point + dir]
            RunCommand(cmd, args)

    # Now we ask beadm to unmount it.
    args = ["unmount", "-f", name]

    if RunCommand(beadm, args) is False:
        return False

    if mount_point is not None:
        try:
            os.rmdir(mount_point)
        except:
            pass
    return True


def DeleteClone(name):
    # Delete the clone we created.

    _CheckBEName(name)
    
    clone = FindClone(name)
    if clone is None:
        return False
    
    args = ["destroy", "-F", clone["realname"]]
    rv = RunCommand(beadm, args)
    if rv is False:
        return rv
    
    return rv


def GetUpdateChanges(old_manifest, new_manifest, cache_dir=None):
    """
    This is used by both PendingUpdatesChanges() and CheckForUpdates().
    The difference between the two is that the latter doesn't necessarily
    have a cache directory, so if cache_dir is none, we have to assume the
    update package exists.
    This returns a dictionary that will have at least "Reboot" as a key.
    """
    def MergeServiceList(base_list, new_list):
        """
        Merge new_list into base_list.
        For each service in new_list (which is a dictionary),
        if the value is True, add it to base_list.
        If new_list is an array, simply add each item to
        base_list; if it's a dict, we check the value.
        """
        if new_list is None:
            return base_list
        if isinstance(new_list, list):
            for svc in new_list:
                if svc not in base_list:
                    base_list.append(svc)
        elif isinstance(new_list, dict):
            for svc, val in new_list.items():
                if val:
                    if svc not in base_list:
                        base_list.append(svc)
        return base_list

    svcs = []
    diffs = Manifest.DiffManifests(old_manifest, new_manifest)
    if len(diffs) == 0:
        return None

    reboot = False
    if REQUIRE_REBOOT:
        reboot = True

    if "Packages" in diffs:
        # Look through the install/upgrade packages
        for pkg, op, old in diffs["Packages"]:
            if op == "delete":
                continue
            if op == "install":
                if pkg.RequiresReboot() == True:
                    reboot = True
                else:
                    pkg_services = pkg.RestartServices()
                    if pkg_services:
                        svcs = MergeServiceList(svcs, pkg_services)
            elif op == "upgrade":
                # A bit trickier.
                # If there is a list of services to restart, the update
                # path wants to look at that rather than the requires reboot.
                # However, one service could be to reboot (this is handled
                # below).
                upd = pkg.Update(old.Version())
                if cache_dir:
                    update_fname = os.path.join(cache_dir, pkg.FileName(old.Version()))
                else:
                    update_fname = None

                if upd and (update_fname is None or os.path.exists(update_fname)):
                    pkg_services = upd.RestartServices()
                    if pkg_services:
                        svcs = MergeServiceList(svcs, pkg_services)
                    else:
                        if upd.RequiresReboot() == True:
                            reboot = True
                else:
                    # Have to assume the full package exists
                    if pkg.RequiresReboot() == True:
                        reboot = True
                    else:
                        pkg_services = pkg.RestartServices()
                        if pkg_services:
                            svcs = MergeServiceList(svcs, pkg_services)
    else:
        reboot = False
    if len(diffs) == 0:
        return None
    if not reboot and svcs:
        if not VerifyServices(svcs):
            reboot = True
        else:
            diffs["Restart"] = svcs
    diffs["Reboot"] = reboot
    return diffs


def CheckForUpdates(handler=None, train=None, cache_dir=None, diff_handler=None):
    """
    Check for an updated manifest.  If cache_dir is none, then we try
    to download just the latest manifest for the given train, and
    compare it to the current system.  If cache_dir is set, then we
    use the manifest in that directory.
    """

    conf = Configuration.SystemConfiguration()
    new_manifest = None
    mfile = None
    if cache_dir:
        try:
            mfile = VerifyUpdate(cache_dir)
            if mfile is None:
                return None
        except UpdateBusyCacheException:
            log.debug("Cache directory %s is busy, so no update available" % cache_dir)
            return None
        except UpdateIncompleteCacheException:
            log.debug("Incomplete cache directory, will try continuing")
        except UpdateInvalidCacheException as e:
            log.error("CheckForUpdate(train = %s, cache_dir = %s):  Got exception %s, removing cache" % (train, cache_dir, str(e)))
            RemoveUpdate(cache_dir)
            return None
        except BaseException as e:
            log.error("CheckForUpdate(train=%s, cache_dir = %s):  Got exception %s" % (train, cache_dir, str(e)))
            raise e
        if mfile:
            # We always want a valid signature when doing an update
            new_manifest = Manifest.Manifest(require_signature=True)
            try:
                new_manifest.LoadFile(mfile)
                mfile.close()
            except Exception as e:
                log.error("Could not load manifest due to %s" % str(e))
                raise e
    else:
        try:
            new_manifest = conf.FindLatestManifest(train=train, require_signature=True)
        except Exception as e:
            log.error("Could not find latest manifest due to %s" % str(e))

    if new_manifest is None:
        raise UpdateManifestNotFound("Manifest could not be found!")

    # If new_manifest is not the requested train, then we don't have an update to do
    if train and train != new_manifest.Train():
        log.debug(
            "CheckForUpdate(train = {0}, cache_dir = {1}):  Wrong train in cache ({2})".format(
                train, cache_dir, new_manifest.Train()
            )
        )
        return None

    # See if the validation script is happy
    new_manifest.RunValidationProgram(cache_dir)

    diffs = GetUpdateChanges(conf.SystemManifest(), new_manifest)
    if diffs is None or len(diffs) == 0:
        return None
    log.debug("CheckForUpdate:  diffs = %s" % diffs)
    if "Packages" in diffs:
        for (pkg, op, old) in diffs["Packages"]:
            if handler:
                handler(op, pkg, old)
    if diff_handler:
        diff_handler(diffs)

    return new_manifest


def DownloadUpdate(train, directory, get_handler=None,
                   check_handler=None, pkg_type=None,
                   ignore_space=False):
    """
    Download, if necessary, the LATEST update for train; download
    delta packages if possible.  Checks to see if the existing content
    is the right version.  In addition to the current caching code, it
    will also stash the current sequence when it downloads; this will
    allow it to determine if a reboot into a different boot environment
    has happened.  This will remove the existing content if it decides
    it has to redownload for any reason.
    Returns True if an update is available, False if no update is avialbale.
    Raises exceptions on errors.
    """

    conf = Configuration.SystemConfiguration()
    mani = conf.SystemManifest()
    # First thing, let's get the latest manifest
    try:
        latest_mani = conf.FindLatestManifest(train, require_signature=True)
    except ManifestInvalidSignature as e:
        log.error("Latest manifest has invalid signature: %s" % str(e))
        raise e

    if latest_mani is None:
        # This probably means we have no network.  Which means we have
        # to trust what we've already downloaded, if anything.
        log.error("Unable to find latest manifest for train %s" % train)
        try:
            VerifyUpdate(directory)
            log.debug("Possibly with no network, cached update looks good")
            return True
        except UpdateIncompleteCacheException:
            log.debug("Possibly with no network, cached update is incomplete")
            raise
        except:
            log.debug("Possibly with no network, either no cached update or it is bad")
            raise

    cache_mani = Manifest.Manifest(require_signature=True)
    mani_file = None
    try:
        try:
            mani_file = VerifyUpdate(directory)
            if mani_file:
                cache_mani.LoadFile(mani_file)
                if cache_mani.Sequence() == latest_mani.Sequence():
                    # Woohoo!
                    mani_file.close()
                    log.debug("DownloadUpdate:  Cache directory has latest manifest")
                    return True
                # Not the latest
                mani_file.close()
            mani_file = None
        except UpdateBusyCacheException:
            msg = "Cache directory %s is busy, so no update available" % directory
            log.debug(msg)
            raise UpdateBusyCacheException(msg)
        except UpdateIncompleteCacheException:
            log.debug("Incomplete cache directory, will try continuing")
            # Hm, this is wrong.  I need to load the manifest file somehow
        except (UpdateInvalidCacheException, ManifestInvalidSignature) as e:
            # It's incomplete, so we need to remove it
            log.error("DownloadUpdate(%s, %s):  Got exception %s; removing cache" % (train, directory, str(e)))
            RemoveUpdate(directory)
        except BaseException as e:
            log.error("Got exception %s while trying to prepare update cache" % str(e))
            raise e

        # If we're dealing with an interrupted download, then the directory will
        # exist, and there may be a MANIFEST file.  So let's try seeing if it
        # does exist, and then compare.
        log.debug("Going to try checking cached manifest %s" % os.path.join(directory, "MANIFEST"))
        try:
            mani_file = open(os.path.join(directory, "MANIFEST"), "r+b")
            try:
                fcntl.lockf(mani_file, fcntl.LOCK_EX | fcntl.LOCK_NB, 0, 0)
            except (IOError, Exception) as e:
                msg = "Unable to lock manifest file: %s" % str(e)
                log.debug(msg)
                mani_file.close()
                raise UpdateBusyCacheException(msg)

            temporary_manifest = Manifest.Manifest(require_signature=True)
            log.debug("Going to try loading manifest file now")
            temporary_manifest.LoadFile(mani_file)
            log.debug("Loaded manifest file")
            log.debug("Cached manifest file has sequence %s, latest_manfest has sequence %s" % (temporary_manifest.Sequence(), latest_mani.Sequence()))
            if temporary_manifest.Sequence() != latest_mani.Sequence():
                mani_file.close()
                log.debug("Cached sequence is not the latest, so removing")
                RemoveUpdate(directory)
                mani_file = None
        except BaseException as e:
            # This could just be that the file doesn't exist, so we don't pass on the exception
            mani_file = None
            log.debug("Got this exception: %s" % str(e))

        if mani_file is None:
            try:
                os.makedirs(directory)
            except BaseException as e:
                log.debug("Unable to create directory %s: %s" % (directory, str(e)))
                log.debug("Hopefully the current cache is okay")

            try:
                mani_file = open(directory + "/MANIFEST", "w+b")
            except (IOError, Exception) as e:
                log.error("Unale to create manifest file in directory %s" % (directory, str(e)))
                raise e

            try:
                fcntl.lockf(mani_file, fcntl.LOCK_EX | fcntl.LOCK_NB, 0, 0)
            except (IOError, Exception) as e:
                msg = "Unable to lock manifest file: %s" % str(e)
                log.debug(msg)
                mani_file.close()
                raise UpdateBusyCacheException(msg)
            # Store the latest manifest.
            latest_mani.StoreFile(mani_file)
            mani_file.flush()

        # Run the update validation, if any.
        # Note that this downloads the file if it's not already there.
        latest_mani.RunValidationProgram(directory, kind=Manifest.VALIDATE_UPDATE)

        # Find out what differences there are
        diffs = Manifest.DiffManifests(mani, latest_mani)
        if diffs is None or len(diffs) == 0:
            log.debug("DownloadUpdate:  No update available")
            # Remove the cache directory and empty manifest file
            RemoveUpdate(directory)
            mani_file.close()
            return False
        log.debug("DownloadUpdate:  diffs = %s" % diffs)

        download_packages = []
        reboot_required = True
        if "Reboot" in diffs:
            reboot_required = diffs["Reboot"]

        if "Packages" in diffs:
            for pkg, op, old in diffs["Packages"]:
                if op == "delete":
                    continue
                log.debug("DownloadUpdate:  Will %s package %s" % (op, pkg.Name()))
                download_packages.append(pkg)

        log.debug("Update does%s seem to require a reboot" % "" if reboot_required else " not")

        # Next steps:  download the package files.
        for indx, pkg in enumerate(download_packages):
            # This is where we find out for real if a reboot is required.
            # To do that, we may need to know which update was downloaded.
            if check_handler:
                check_handler(indx + 1, pkg=pkg, pkgList=download_packages)
            pkg_file = conf.FindPackageFile(
                pkg, save_dir=directory, handler=get_handler, pkg_type=pkg_type,
                ignore_space=ignore_space
            )
            if pkg_file is None:
                log.error("Could not download package file for %s" % pkg.Name())
                RemoveUpdate(directory)
                return False
            else:
                pkg_file.close()

        # Almost done:  get a changelog if one exists for the train
        # If we can't get it, we don't care.
        try:
            with conf.GetChangeLog(train, save_dir=directory, handler=get_handler):
                pass
        except AttributeError:
            # GetChangeLog can return None, which throws things, no pun intended
            pass
        # Then save the manifest file.
        # Create the SEQUENCE file.
        with open(directory + "/SEQUENCE", "w") as f:
            f.write("%s" % conf.SystemManifest().Sequence())
        # And create the SERVER file.
        with open(directory + "/SERVER", "w") as f:
            f.write("%s" % conf.UpdateServerName())

    finally:
        if mani_file:
            mani_file.close()

    # if no error has been raised so far Then return True!
    return True


def PendingUpdates(directory):
    import traceback
    try:
        changes = PendingUpdatesChanges(directory)
        if changes is None or len(changes) <= 1:
            return False
        else:
            return True
    except:
        log.debug("PendingUpdatesChanges raised exception %s" % sys.exc_info()[0])
        traceback.print_exc()
        return False


def PendingUpdatesChanges(directory):
    """
    Return a list (a la CheckForUpdates handler right now) of
    changes between the currently installed system and the
    downloaded contents in <directory>.  If <directory>'s values
    are incomplete or invalid for whatever reason, return
    None.  "Incomplete" means a necessary file for upgrading
    from the current system is not present; "Invalid" means that
    one part of it is invalid -- manifest is not valid, signature isn't
    valid, checksum for a file is invalid, or the stashed sequence
    number does not match the current system's sequence.
    """
    mani_file = None
    conf = Configuration.SystemConfiguration()
    try:
        mani_file = VerifyUpdate(directory)
    except UpdateBusyCacheException:
        log.debug("Cache directory %s is busy, so no update available" % directory)
        raise
    except UpdateIncompleteCacheException as e:
        log.error(str(e))
        raise
    except UpdateInvalidCacheException as e:
        log.error(str(e))
        RemoveUpdate(directory)
        raise
    except BaseException as e:
        log.error("Got exception %s while trying to determine pending updates" % str(e))
        raise
    if mani_file:
        new_manifest = Manifest.Manifest(require_signature=True)
        try:
            new_manifest.LoadFile(mani_file)
        except ManifestInvalidSignature as e:
            log.error("Invalid signature in cached manifest: %s" % str(e))
            raise
        finally:
            mani_file.close()
        # This returns a set of differences.
        # But we shouldn't rely on it until we can look at what we've
        # actually downloaded.  To do that, we need to look at any
        # package differences (diffs["Packages"]), and check the
        # updates if that's what got downloaded.
        # By definition, if there are no Packages differences, a reboot
        # isn't required.
        diffs = GetUpdateChanges(conf.SystemManifest(), new_manifest, cache_dir=directory)
        return diffs
    else:
        return None


def ServiceRestarts(directory):
    """
    Return a list of services to be stopped and started.  The paramter
    directory is the cache location; if it's not a valid cache directory,
    we return None.  (This is different from returning an empty set,
    which will be an array with no items.)  If a reboot is required,
    it returns an empty array.
    """
    changes = PendingUpdatesChanges(directory)
    if changes is None:
        return None
    retval = []
    if changes["Reboot"] is False:
        # Only look if we don't need to reboot
        if "Packages" in changes:
            # All service changes are package-specific
            for (pkg, op, old) in changes["Packages"]:
                svcs = None
                if op in ("install", "delete"):
                    # Either the service is added or removed,
                    # either way we add it to the list.
                    svcs = pkg.RestartServices()
                elif op == "upgrade":
                    # We need to see if we have the delta package
                    # file or not.
                    delta_pkg_file = os.path.join(directory, pkg.FileName(old.Version()))
                    if os.path.exists(delta_pkg_file):
                        # Okay, we're doing an update
                        upd = pkg.Update(old.Version())
                        if not upd:
                            # How can this happen?
                            raise Exception("I am confused")
                        svcs = upd.RestartServices()
                    else:
                        # Only need to the services listed at the outer level
                        svcs = pkg.RestartServices()

                if svcs:
                    for svc in svcs:
                        if svc not in retval:
                            retval.append(svc)

    return retval

def ExtractFrozenUpdate(tarball, dest_dir, verbose=False):
    """
    Extract the files in the given tarball into dest_dir.
    This assumes dest_dir already exists.
    """
    extracted = False
    conf = Configuration.SystemConfiguration()
    try:
        with tarfile.open(tarball) as tf:
            files = tf.getmembers()
            for f in files:
                if f.name in ("./", ".", "./."):
                    continue
                if not f.name.startswith("./"):
                    if verbose:
                        log.debug("Illegal member {0}".format(f))
                    continue
                if len(f.name.split("/")) != 2:
                    if verbose:
                        log.debug("Illegal member name {0} has too many path components".format(f.name))
                    continue
                if verbose:
                    log.debug("Extracting {0}".format(f.name))
                tf.extract(f.name, path=dest_dir)
                extracted = True
                if verbose:
                    log.debug("Done extracting {0}".format(f.name))
    except tarfile.TarError:
        raise Exceptions.UpdateBadFrozenFile("Bad tar file {0}".format(tarball))
    if extracted:
        # We've extracted some files, and it may be an updated!
        with open(os.path.join(dest_dir, "SEQUENCE"), "w") as s:
            s.write(conf.SystemManifest().Sequence())
        with open(os.path.join(dest_dir, "SERVER"), "w") as s:
            s.write(conf.UpdateServerName())
    return True


def ApplyUpdate(directory,
                install_handler=None,
                force_reboot=False,
                ignore_space=False,
                progressFunc=None,
                force_trampoline=None
                ):
    """
    Apply the update in <directory>.  As with PendingUpdates(), it will
    have to verify the contents before it actually installs them, so
    it has the same behaviour with incomplete or invalid content.
    """
    rv = False
    conf = Configuration.SystemConfiguration()
    # Note that PendingUpdates may raise an exception
    changes = PendingUpdatesChanges(directory)

    if changes is None:
        # This means no updates to apply, and so nothing to do.
        return None

    # Do I have to worry about a race condition here?
    new_manifest = Manifest.Manifest(require_signature=True)
    try:
        new_manifest.LoadPath(directory + "/MANIFEST")
    except ManifestInvalidSignature as e:
        log.error("Cached manifest has invalid signature: %s" % str(e))
        raise e

    # Run the update validation, if any.  This may
    # raise an exception.
    new_manifest.RunValidationProgram(directory)
    conf.SetPackageDir(directory)

    # If we're here, then we have some change to make.
    # PendingUpdatesChanges always sets this, unless it returns None
    reboot = changes["Reboot"]
    if force_reboot:
        # Just in case
        reboot = True
    if REQUIRE_REBOOT:
        # In case we have globally disabled rebootless updates
        reboot = True
    changes.pop("Reboot")
    if len(changes) == 0:
        # This shouldn't happen
        log.debug("ApplyUpdate: changes only has Reboot key")
        return None

    service_list = None
    deleted_packages = []
    updated_packages = []
    if "Packages" in changes:
        for (pkg, op, old) in changes["Packages"]:
            if op == "delete":
                log.debug("Delete package %s" % pkg.Name())
                deleted_packages.append(pkg)
                continue
            elif op == "install":
                log.debug("Install package %s" % pkg.Name())
                updated_packages.append(pkg)
            elif op == "upgrade":
                log.debug("Upgrade package %s-%s to %s-%s" % (old.Name(), old.Version(), pkg.Name(), pkg.Version()))
                updated_packages.append(pkg)
            else:
                log.error("Unknown package operation %s for %s" % (op, pkg.Name()))

    if new_manifest.Version().startswith(Avatar() + "-"):
        new_boot_name = new_manifest.Version()[len(Avatar() + "-"):]
    else:
        new_boot_name = "%s-%s" % (Avatar(), new_manifest.Version()[len(Avatar() + "-"):])

    log.debug("new_boot_name = %s, reboot = %s" % (new_boot_name, reboot))

    installer = Installer.Installer(
        manifest=new_manifest,
        config=conf
    )
    if force_trampoline is not None:
        log.debug("ApplyUpdate: force_trampoline = {} (bool {})".format(force_trampoline, bool(force_trampoline)))
        installer.trampoline = bool(force_trampoline)

    installer.GetPackages(pkgList=updated_packages)
    log.debug("Installer got packages %s" % installer.Packages())
    
    """
    There is no way around this:  this is a horrible hack.  It
    only works with gzipped files, the module for which, for some
    reason, does not have a function or method to get the uncompressed
    size.
    """
    def ActualSize(gzf):
        try:
            import struct
            cur = gzf.tell()
            gzf.seek(-4, 2)    # Last 4 bytes have the uncompressed size
            (rv,) = struct.unpack("<I", gzf.read(4))
            gzf.seek(cur, 0)
        except:
            rv = os.fstat(gzf.fileno()).st_size
        return rv

    space_needed = 0
    for f in installer.Packages():
        [(dc, fobj)] = f.items()
        try:
            space_needed += ActualSize(fobj)
        except:
            pass
        
    if not ignore_space and not PruneClones(required=space_needed):
        raise UpdateInsufficientSpace("Insufficent space to install update")
    
    mount_point = None
    if reboot:
        # Need to create a new boot environment
        try:
            count = 0
            create_name = new_boot_name
            while count < 500:
                try:
                    rv = CreateClone(create_name)
                    break
                except KeyError:
                    count = count + 1
                    create_name = "{0}-{1}".format(new_boot_name, count)
                    rv = False
                    continue
            
            new_boot_name = create_name
            if rv is False:
                log.debug("Failed to create BE %s" % create_name)
                # It's possible the boot environment already exists.
                s = None
                clones = ListClones()
                if clones:
                    found = False
                    for c in clones:
                        if c["name"] == new_boot_name:
                            found = True
                            if c["mountpoint"] == "/":
                                s = "Cannot create boot-environment with same name as current boot-environment (%s)" % new_boot_name
                                break
                            elif c["active"] in ("R", "NR"):
                                s = "Cannot destroy boot-environment selected for next reboot (%s)" % new_boot_name
                            else:
                                # We'll have to destroy it.
                                # I'd like to rename it, but that gets tricky, due
                                # to nicknames.
                                if DeleteClone(new_boot_name) == False:
                                    s = "Cannot destroy BE %s which is necessary for upgrade" % new_boot_name
                                    log.debug(s)
                                elif CreateClone(new_boot_name) is False:
                                    s = "Cannot create new BE %s even after a second attempt" % new_boot_name
                                    log.debug(s)
                            break
                    if found is False:
                        s = "Unable to create boot-environment %s" % new_boot_name
                else:
                    log.debug("Unable to list clones after creation failure")
                    s = "Unable to create boot-environment %s" % new_boot_name
                if s:
                    log.error(s)
                    raise UpdateBootEnvironmentException(s)
            if mount_point is None:
                mount_point = MountClone(new_boot_name)
        except:
            mount_point = None
            s = sys.exc_info()[0]
        if mount_point is None:
            s = "Unable to mount boot-environment %s" % new_boot_name
            log.error(s)
            DeleteClone(new_boot_name)
            raise UpdateBootEnvironmentException(s)
    else:
        # Need to do magic to move the current boot environment aside,
        # and assign the newname to the current boot environment.
        # Also need to make a snapshot of the current root so we can
        # clean up on error
        mount_point = None
        log.debug("We should try to do a non-rebooty update")
        root_dataset = GetRootDataset()
        if root_dataset is None:
            log.error("Unable to determine root environment name")
            raise UpdateBootEnvironmentException("Unable to determine root environment name")
        # We also want the root name
        root_env = None
        clones = ListClones()
        if clones is None:
            log.error("Unable to determine root BE")
            raise UpdateBootEnvironmentException("Unable to determine root BE")
        for clone in clones:
            if clone["mountpoint"] == "/":
                root_env = clone
                break
        if root_env is None:
            log.error("Unable to find root BE!")
            raise UpdateBootEnvironmentException("Unable to find root BE!")

        # Now we want to snapshot the current boot environment,
        # so we can rollback as needed.
        snapshot_name = "%s@Pre-Uprgade-%s" % (root_dataset, new_manifest.Sequence())
        cmd = "/sbin/zfs"
        args = ["snapshot", "-r", snapshot_name]
        rv = RunCommand(cmd, args)
        if rv is False:
            log.error("Unable to create snapshot %s, bailing for now" % snapshot_name)
            raise UpdateSnapshotException("Unable to create snapshot %s" % snapshot_name)
        # We need to remove the beadm:nickname property.  I hate knowing this much
        # about the implementation
        args = ["inherit", "-r", "beadm:nickname", snapshot_name]
        RunCommand(cmd, args)

        # At this point, we'd want to rename the boot environment to be the new
        # name, which would be new_manifest.Sequence()
        if CreateClone(new_boot_name, rename=root_env["name"]) is False:
            log.error("Unable to create new boot environment %s" % new_boot_name)
            # Roll back and destroy the snapshot we took
            cmd = "/sbin/zfs"
            args = ["rollback", snapshot_name]
            RunCommand(cmd, args)
            args[0] = "destroy"
            RunCommand(cmd, args)
            # And set the beadm:nickname property back
            args = ["set", "beadm:nickname=%s" % root_env["name"],
                    "freenas-boot/ROOT/{0}".format(root_env["realname"])]

            RunCommand(cmd, args)

            raise UpdateBootEnvironmentException(
                "Unable to create new boot environment {0}".format(new_boot_name)
            )
        if "Restart" in changes:
            service_list = StopServices(changes["Restart"])
    try:
        cl = FindClone(new_boot_name)
    except:
        cl = None
    if cl is None:
        if mount_point:
            try:
                UnmountClone(new_boot_name, mount_point)
                DeleteClone(new_boot_name)
            except:
                log.debug("Got an exception while trying to clean up after FindClone", exc_info=True)
                
        s = "Unable to find BE %s just after creation" % new_boot_name
        log.debug(s)
        raise UpdateBootEnvironmentException(s)
    else:
        if not CloneSetAttr(cl, keep=False, sync="disabled"):
            s = "Unable to set keep attribute on BE %s" % new_boot_name
            log.debug(s)
            
    installer.SetRoot(mount_point)
    
    # Now we start doing the update!
    # If we have to reboot, then we need to
    # make a new boot environment, with the appropriate name.
    # If we are *not* rebooting, then we want to rename the
    # current one with the appropriate name, while at the same
    # time cloning the current one and keeping the existing name.
    # Easy peasy, right?

    try:
        # Remove any deleted packages
        for pkg in deleted_packages:
            log.debug("About to delete package %s from %s" % (pkg.Name(), mount_point))
            if conf.PackageDB(mount_point).RemovePackageContents(pkg.Name()) == False:
                s = "Unable to remove contents for package %s" % pkg.Name()
                if mount_point:
                    UnmountClone(new_boot_name, mount_point)
                    mount_point = None
                    DeleteClone(new_boot_name)
                raise UpdatePackageException(s)
            conf.PackageDB(mount_point).RemovePackage(pkg.Name())

        # Now to start installing the packages
        rv = False
        if installer.InstallPackages(progressFunc=progressFunc, handler=install_handler) is False:
            log.error("Unable to install packages")
            raise UpdatePackageException("Unable to install packages")
        else:
            new_manifest.Save(mount_point)
            if mount_point:
                if not CloneSetAttr(cl, sync=None):
                    log.debug("Unable to clear sync on BE {}".format(cl["realname"]))

                if UnmountClone(new_boot_name, mount_point) is False:
                    s = "Unable to unmount clone environment %s from mount point %s" % (new_boot_name, mount_point)
                    log.error(s)
                    raise UpdateBootEnvironmentException(s)
                mount_point = None
            if reboot:
                if ActivateClone(new_boot_name) is False:
                    s = "Unable to activate clone environment %s" % new_boot_name
                    log.error(s)
                    raise UpdateBootEnvironmentException(s)
            if not reboot:
                # Try to restart services before cleaning up.
                # Although maybe that's not the right way to go
                if service_list:
                    StartServices(service_list)
                    service_list = None
                # Clean up the emergency holographic snapshot
                cmd = "/sbin/zfs"
                args = ["destroy", "-r", snapshot_name]
                rv = RunCommand(cmd, args)
                if rv is False:
                    log.error("Unable to destroy snapshot %s" % snapshot_name)
            RemoveUpdate(directory)
            # RunCommand("/sbin/zpool", ["scrub", "freenas-boot"])
    except BaseException as e:
        # Cleanup code is entirely different for reboot vs non reboot
        log.error("Update got exception during update: %s", e, exc_info=True)
        if reboot:
            if mount_point:
                UnmountClone(new_boot_name, mount_point)
            if new_boot_name:
                DeleteClone(new_boot_name)
        else:
            # Need to roll back
            # We also need to delete the renamed clone of /,
            # and then rename / to the original name.
            # First, however, destroy the clone
            rv = DeleteClone(root_env["name"])
            if rv:
                # Next, rename the clone
                rv = RenameClone(new_boot_name, root_env["name"])
                if rv:
                    # Now roll back the snapshot, and set the beadm:nickname value
                    cmd = "/sbin/zfs"
                    args = ["rollback", "-r", snapshot_name]
                    rv = RunCommand(cmd, args)
                    if rv is False:
                        log.error("Unable to rollback %s" % snapshot_name)
                        # Don't know what to do then
                    args = ["set", "beadm:nickname=%s" % root_env["name"], "freenas-boot/ROOT/%s" % root_env["name"]]
                    rv = RunCommand(cmd, args)
                    if rv is False:
                        log.error("Unable to set nickname, wonder what I did wrong")
                    args = ["destroy", "-r", snapshot_name]
                    rv = RunCommand(cmd, args)
                    if rv is False:
                        log.error("Unable to destroy snapshot %s" % snapshot_name)
            if service_list:
                StartServices(service_list)
        raise e

    return reboot


def VerifyUpdate(directory):
    """
    Verify the update in the directory is valid -- the manifest
    is sane, any signature is valid, the package files necessary to
    update are present, and have a valid checksum.  Returns either
    a file object if it's valid (the file object is locked), None
    if it doesn't exist, or it raises an exception -- one of
    UpdateIncompleteCacheException or UpdateInvalidCacheException --
    if necessary.
    """

    # First thing we do is get the systen configuration and
    # systen manifest
    conf = Configuration.SystemConfiguration()
    mani = conf.SystemManifest()

    # Next, let's see if the directory exists.
    if not os.path.exists(directory):
        return None
    # Open up the manifest file.  Assuming it exists.
    try:
        mani_file = open(directory + "/MANIFEST", "r+")
    except:
        # Doesn't exist.  Or we can't get to it, which would be weird.
        return None
    # Let's try getting an exclusive lock on the manifest
    try:
        fcntl.lockf(mani_file, fcntl.LOCK_EX | fcntl.LOCK_NB, 0, 0)
    except:
        # Well, if we can't acquire the lock, someone else has it.
        # Throw an incomplete exception after closing the file
        mani_file.close()
        raise UpdateBusyCacheException("Cache directory %s is being modified" % directory)

    # We always want a valid signature for an update.
    cached_mani = Manifest.Manifest(require_signature=True)
    try:
        cached_mani.LoadFile(mani_file)
    except Exception as e:
        # If we got an exception, it's invalid.
        mani_file.close()
        log.error("Could not load cached manifest file: %s" % str(e))
        raise UpdateInvalidCacheException


    # First easy thing to do:  look for the SEQUENCE file.
    try:
        with open(directory + "/SEQUENCE", "r") as f:
            cached_sequence = f.read().rstrip()
    except (IOError, Exception) as e:
        mani_file.close()
        log.error("Could not open sequence file in cache directory %s: %s" % (directory, str(e)))
        raise UpdateIncompleteCacheException(
            "Cache directory {0} does not have a sequence file".format(directory)
        )

    # Now let's see if the sequence matches us.
    if cached_sequence != mani.Sequence():
        mani_file.close()
        log.error("Cached sequence, %s, does not match system sequence, %s" % (cached_sequence, mani.Sequence()))
        raise UpdateInvalidCacheException("Cached sequence does not match system sequence")

    # Second easy thing to do:  if there is a SERVER file, make sure it's the same server
    # name we're using
    cached_server = "default"
    try:
        with open(directory + "/SERVER", "r") as f:
            cached_server = f.read().rstrip()
    except (IOError, Exception) as e:
        log.debug("Could not open SERVER file in cache direcory %s: %s" % (directory, str(e)))
        cached_server = "default"

    if cached_server != conf.UpdateServerName():
        mani_file.close()
        log.error("Cached server, %s, does not match system update server, %s" % (cached_server, conf.UpdateServerName()))
        raise UpdateInvalidCacheException("Cached server name does not match system update server")

    # Next, see if the validation script (if any) is there
    validation_program = cached_mani.ValidationProgram(Manifest.VALIDATE_UPDATE)
    if validation_program:
        if not os.path.exists(os.path.join(directory, validation_program["Kind"])):
            mani_file.close()
            log.error("Validation program %s is required, but not in cache directory" % validation_program["Kind"])
            raise UpdateIncompleteCacheException("Cache directory %s missing validation program %s" % (directory, validation_program["Kind"]))

    # Next thing to do is go through the manifest, and decide which package files we need.
    diffs = Manifest.DiffManifests(mani, cached_mani)
    # This gives us an array to examine.
    # All we care about for verification is the packages
    if "Packages" in diffs:
        for (pkg, op, old) in diffs["Packages"]:
            if op == "delete":
                # Deleted package, so we don't need to do any verification here
                continue
            if op == "install":
                # New package, being installed, so we need the full package
                cur_vers = None
            if op == "upgrade":
                # Package being updated, so we can look for the delta package.
                cur_vers = old.Version()
            # This is slightly redundant -- if cur_vers is None, it'll check
            # the same filename twice.
            if not os.path.exists(directory + "/" + pkg.FileName()) and \
               not os.path.exists(directory + "/" + pkg.FileName(cur_vers)):
                mani_file.close()
                # Neither exists, so incoplete
                log.error(
                    "Cache %s directory missing files for package %s" % (directory, pkg.Name())
                )
                raise UpdateIncompleteCacheException(
                    "Cache directory {0} missing files for package {1}".format(directory, pkg.Name())
                )
            # Okay, at least one of them exists.
            # Let's try the full file first
            try:
                with open(directory + "/" + pkg.FileName(), 'rb') as f:
                    if pkg.Checksum():
                        cksum = Configuration.ChecksumFile(f)
                        if cksum == pkg.Checksum():
                            continue
                    else:
                        continue
            except:
                pass

            if cur_vers is None:
                e = "Cache directory %s missing files for package %s" % (directory, pkg.Name())
                log.error(e)
                raise UpdateIncompleteCacheException(e)

            # Now we try the delta file
            # To do that, we need to find the right dictionary in the pkg
            upd_cksum = None
            update = pkg.Update(cur_vers)
            if update and update.Checksum():
                upd_cksum = update.Checksum()
                try:
                    with open(directory + "/" + pkg.FileName(cur_vers), 'rb') as f:
                        cksum = Configuration.ChecksumFile(f)
                        if upd_cksum != cksum:
                            update = None
                except:
                    update = None
            if update is None:
                mani_file.close()
                # If we got here, we are missing this file
                log_msg = "Cache directory %s is missing package %s" % (directory, pkg.Name())
                log.error(log_msg)
                raise UpdateIncompleteCacheException(log_msg)
        # And end that loop
    # And if we got here, then we have found all of the packages, the manifest is fine,
    # and the sequence tag is correct.
    mani_file.seek(0)
    return mani_file


def RemoveUpdate(directory):
    try:
        shutil.rmtree(directory)
    except:
        pass
    return
