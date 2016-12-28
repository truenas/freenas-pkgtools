#!/usr/local/bin/python
from __future__ import print_function
import sys

import getopt

sys.path.append("/usr/local/lib")

import freenasOS.Manifest as Manifest
import freenasOS.Package as Package
import freenasOS.Configuration as Configuration
import freenasOS.Installer as Installer

class ProgressHandler(object):
    def __init__(self):
        self.percent = 0

    def __enter__(self):
        return self

    def __exit__(self, type, value, traceback):
        pass
    
    def update(self, **kwargs):
        total = kwargs.pop("total", 0)
        index = kwargs.pop("index", 0)
        name = kwargs.pop("name", None)
        done = kwargs.pop("done", False)
        if done:
            if self.percent < 100:
                print("100")
            else:
                print("")
            self.percent = 0
        elif total:
            cur_pct = int((index * 100) / total)
#            print("index={}, total={}, self.percent={}, cur_pct={}".format(index, total, self.percent, cur_pct))
            if cur_pct > self.percent:
                self.percent = cur_pct
                if self.percent % 10 == 0:
                    print("{}".format(self.percent), end="")
                elif self.percent % 2 == 0:
                    print(".", end="")
                sys.stdout.flush()

def install_handler(index, name, packages):
    print("Installing {0} ({1} of {2})".format(name, index, len(packages)))
    
def usage():
    print("Usage: %s -M manifest [-P package_dir] root" % sys.argv[0], file=sys.stderr)
    print("\tNote:  package dir is parent of Packages directory", file=sys.stderr)
    sys.exit(1)

if __name__ == "__main__":
    mani_file = None
    package_dir = None
    try:
        opts, args = getopt.getopt(sys.argv[1:], "M:P:")
    except getopt.GetoptError as err:
        print(str(err), file=sys.stderr)
        usage()

    for (o, a) in opts:
        if o == "-M":
            mani_file = a
        elif o == "-P":
            package_dir = a
        else:
            usage()

    if len(args) != 1:
        usage()

    root = args[0]

    config = Configuration.Configuration()
    if package_dir is not None:
        config.SetPackageDir(package_dir)

    if mani_file is None:
        manifest = config.SystemManifest()
    else:
        # We ignore the signature because freenas-install is
        # called from the ISO install, and the GUI install, which
        # have their own checksums elsewhere.
        manifest = Manifest.Manifest(config, require_signature=False)
        manifest.LoadPath(mani_file)

    installer = Installer.Installer(manifest=manifest, root=root, config=config)

    if installer.GetPackages() is not True:
        print("Huh, could not install and yet it returned", file=sys.stderr)

    with ProgressHandler() as pf:
        # For installation, we assume that we're running the same kernel as the new system.
        installer.trampoline = False
        installer.InstallPackages(progressFunc=pf.update, handler=install_handler)

    manifest.Save(root)
    sys.exit(0)
