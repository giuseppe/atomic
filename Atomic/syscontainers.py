import os
import sys
import json
from . import util
import tempfile
import tarfile
from string import Template
import calendar
import shutil
import stat # pylint: disable=bad-python3-import
import subprocess
import time
import errno
from .client import AtomicDocker
from Atomic.backends._docker_errors import NoDockerDaemon
from ctypes import cdll, CDLL
import uuid
from .rpm_host_install import RPMHostInstall, RPM_NAME_PREFIX
import __main__

try:
    import gi
    try:
        gi.require_version('OSTree', '1.0')
        from gi.repository import Gio, GLib, OSTree  # pylint: disable=no-name-in-module
        OSTREE_PRESENT = True
    except ValueError:
        OSTREE_PRESENT = False
except ImportError:
    OSTREE_PRESENT = False

try:
    from subprocess import DEVNULL  # pylint: disable=no-name-in-module
except ImportError:
    DEVNULL = open(os.devnull, 'wb')

HOME = os.path.expanduser("~")

ATOMIC_LIBEXEC = os.environ.get('ATOMIC_LIBEXEC', '/usr/libexec/atomic')
ATOMIC_VAR = '/var/lib/containers/atomic'
ATOMIC_USR = '/usr/lib/containers/atomic'
ATOMIC_VAR_USER = "%s/.containers/atomic" % HOME
OSTREE_OCIIMAGE_PREFIX = "ociimage/"
SYSTEMD_UNIT_FILES_DEST = "/etc/systemd/system"
SYSTEMD_UNIT_FILES_DEST_USER = "%s/.config/systemd/user" % HOME
SYSTEMD_TMPFILES_DEST = "/etc/tmpfiles.d"
SYSTEMD_TMPFILES_DEST_USER = "%s/.containers/tmpfiles" % HOME
SYSTEMD_UNIT_FILES_DEST_PREFIX = "%s/usr/lib/systemd/system"
SYSTEMD_TMPFILES_DEST_PREFIX = "%s/usr/lib/tmpfiles.d"
SYSTEMD_UNIT_FILE_DEFAULT_TEMPLATE = """
[Unit]
Description=$NAME

[Service]
ExecStartPre=$EXEC_STARTPRE
ExecStart=$EXEC_START
ExecStop=$EXEC_STOP
ExecStopPost=$EXEC_STOPPOST
Restart=on-failure
WorkingDirectory=$DESTDIR
PIDFile=$PIDFILE

[Install]
WantedBy=multi-user.target
"""
TEMPLATE_FORCED_VARIABLES = ["DESTDIR", "NAME", "EXEC_START", "EXEC_STOP",
                             "EXEC_STARTPRE", "EXEC_STOPPOST", "HOST_UID",
                             "HOST_GID", "IMAGE_ID", "IMAGE_NAME"]
TEMPLATE_OVERRIDABLE_VARIABLES = ["RUN_DIRECTORY", "STATE_DIRECTORY", "CONF_DIRECTORY", "UUID", "PIDFILE",
                                  "ALL_PROCESS_CAPABILITIES"]


class SystemContainers(object):
    """
    Provides an interface for manipulating system containers.
    """

    def __init__(self):
        """
        Initializes a new instance of the SystemContainers class.
        """
        self.atomic_config = util.get_atomic_config()
        self.backend = None
        self.user = util.is_user_mode()
        self.args = None
        self.setvalues = None
        self.display = False
        self.runtime = None
        self._repo_location = None
        self._runtime_from_info_file = None

    def get_atomic_config_item(self, config_item):
        """
        Returns a specific configuration item from the atomic configuration file.

        :param config_item: Items to retrieve from the config.
        :type config_item: list
        :rtype: mixed
        """
        return util.get_atomic_config_item(config_item, atomic_config=self.atomic_config)

    def _do_syncfs(self, rootfs, rootfs_fd):
        # Fallback to sync --file-system if loading it from libc fails.
        try:
            cdll.LoadLibrary("libc.so.6")
            libc = CDLL("libc.so.6")
            if libc.syncfs(rootfs_fd) == 0:
                return
        except (NameError, AttributeError, OSError):
            pass

        util.check_call(["sync", "--file-system", rootfs], stdin=DEVNULL,
                        stdout=DEVNULL,
                        stderr=DEVNULL)

    @property
    def available(self):
        return OSTREE_PRESENT

    def _checkout_layer(self, repo, rootfs_fd, rootfs, rev):
        options = OSTree.RepoCheckoutAtOptions() # pylint: disable=no-member
        options.overwrite_mode = OSTree.RepoCheckoutOverwriteMode.UNION_FILES
        options.process_whiteouts = True
        options.disable_fsync = True
        options.no_copy_fallback = True
        if self.user:
            options.mode = OSTree.RepoCheckoutMode.USER
        repo.checkout_at(options, rootfs_fd, rootfs, rev)

    def set_args(self, args):
        """
        Sets arguments used by other methods. Generally populated via CLI.

        :param args: Argument instance
        :type args: argparse.Namespace or object
        """
        self.args = args

        try:
            self.backend = args.backend
        except (NameError, AttributeError):
            self.backend = None
        if not self.backend:
            self.backend = self.get_atomic_config_item(["default_storage"]) or "ostree"

        try:
            self.display = self.args.display
        except (NameError, AttributeError):
            pass

        try:
            self.setvalues = args.setvalues
        except (NameError, AttributeError):
            pass

        try:
            self.runtime = self.args.runtime
        except (NameError, AttributeError):
            pass

        if not self.user and not self.runtime:
            self.runtime = self.get_atomic_config_item(["runtime"])


    @staticmethod
    def _split_set_args(setvalues):
        values = {}
        for i in setvalues:
            split = i.find("=")
            if split < 0:
                raise ValueError("Invalid value '%s'.  Expected form NAME=VALUE" % i)
            key, val = i[:split], i[split+1:]
            values[key] = val
        return values

    def _pull_image_to_ostree(self, repo, image, upgrade, src_creds=None):
        if not repo:
            raise ValueError("Cannot find a configured OSTree repo")
        if image.startswith("ostree:") and image.count(':') > 1:
            self._check_system_ostree_image(repo, image, upgrade)
        elif image.startswith("docker:") and image.count(':') > 1:
            image = self._pull_docker_image(repo, image.replace("docker:", "", 1))
        elif image.startswith("dockertar:/"):
            tarpath = image.replace("dockertar:/", "", 1)
            image = self._pull_docker_tar(repo, tarpath, os.path.basename(tarpath).replace(".tar", ""))
        else: # Assume "oci:"
            self._check_system_oci_image(repo, image, upgrade, src_creds=src_creds)
        return image

    def pull_image(self, image=None, **kwargs):
        """
        Public method for pulling an image from an external location into ostree.

        :param image: Name of the image to pull. If not provided self.args.image is used.
        :type image: str or None

        :param kwargs: Arguments to pass to the pull
        :type kwargs: dict or None
        """
        src_creds = kwargs.get('src_creds')
        self._pull_image_to_ostree(self._get_ostree_repo(), image or self.args.image, True, src_creds=src_creds)

    def install_user_container(self, image, name):
        """
        Installs a new user container.

        :param image: Name of the image to use to create the new user container.
        :type image: str
        :param name: The name to call the new user container.
        :type name: str
        :returns: Shell call result from self.install()
        :rtype: int
        """
        try:
            runtime = self._get_oci_runtime()
            util.check_call([runtime, "--version"], stdout=DEVNULL)
        except util.FileNotFound:
            raise ValueError("Cannot install the container: the runtime {} is not installed".format(runtime))

        if not "--user" in str(util.check_output(["systemctl", "--help"], stdin=DEVNULL, stderr=DEVNULL)):
            raise ValueError("Cannot install the container: systemctl does not support --user")

        # Same entrypoint
        return self.install(image, name)

    def build_rpm(self, repo, name, image, values, destination):
        """
        Create a checkout and generate an RPM file

        :param repo: OSTree repo used to checkout contents.
        :type repo: OSTree.Repo
        :param name: Name of the container to use when building.
        :type name: str
        :param image: Name of the image to build the container and rpm from.
        :type image: str
        :param values: Values to be used when expanding templates.
        :type values: dict
        :param destination: Location on disk to place the generated rpm.
        :type destination: str
        :returns: The path to the new rpm or None
        :rtype: str or None
        """
        installed_files = None
        temp_dir = tempfile.mkdtemp()
        rpm_content = os.path.join(temp_dir, "rpmroot")
        rootfs = os.path.join(rpm_content, "usr/lib/containers/atomic", name)
        os.makedirs(rootfs)
        try:
            self._checkout(repo, name, image, 0, False, values=values, destination=rootfs, prefix=rpm_content)
            if self.display:
                return None
            img = self.inspect_system_image(image)
            if installed_files is None:
                with open(os.path.join(rootfs, "info"), "r") as info_file:
                    info = json.loads(info_file.read())
                    installed_files = info["installed-files"] if "installed-files" in info else None

            image_id = img["ImageId"]
            labels = {k.lower() : v for k, v in img.get('Labels', {}).items()}
            ret = RPMHostInstall.generate_rpm_from_rootfs(rootfs, temp_dir, name, image_id, labels, True, installed_files=installed_files, display=self.display)
            if ret:
                rpm_built = RPMHostInstall.find_rpm(ret)
                generated_rpm = os.path.join(destination, os.path.basename(rpm_built))
                shutil.move(rpm_built, generated_rpm)
                return generated_rpm
        finally:
            shutil.rmtree(temp_dir)
        return None

    def install(self, image, name):
        """
        External container install logic.

        :param image: The name of the image
        :type image: str
        :param name: The name of the checkout
        :type name: str
        :returns: Shell call result
        :rtype: int
        """
        return_value = None

        try:
            runtime = self._get_oci_runtime()
            util.check_call([runtime, "--version"], stdout=DEVNULL)
        except util.FileNotFound:
            raise ValueError("Cannot install the container: the runtime {} is not installed".format(runtime))

        # If we don't have a dockertar file or a reference to a docker engine image
        if not image.startswith('dockertar:/') and not (image.startswith("docker:") and image.count(':') > 1):
            image = util.remove_skopeo_prefixes(image)
            labels = self.inspect_system_image(image).get('Labels', {})
            # And we have a run-once label
            if labels and labels.get('atomic.run') == 'once':
                # Execute the _run_once method and set the return_value
                return_value = self._run_once(image, name)
        # If we don't have a return_value then use the traditional install
        if return_value is None:
            return_value = self._install(image, name)
        # Return
        return return_value

    def _run_once(self, image, name):
        """
        Runs the container once and then removes it.

        :param image: The name of the image
        :type image: str
        :param name: The name of the checkout
        :type name: str
        :returns: Shell call result
        :rtype: int
        """
        # Create a temporary directory to house the oneshot container
        base_dir = os.path.join(self.get_ostree_repo_location(), "tmp/atomic-container", str(os.getpid()))
        tmpfiles_destination = None
        mounted_from_storage = False
        try:
            rootfs = os.path.sep.join([base_dir, 'rootfs'])
            os.makedirs(rootfs)
            try:
                upperdir = os.path.sep.join([base_dir, 'upperdir'])
                workdir = os.path.sep.join([base_dir, 'workdir'])
                for i in [upperdir, workdir]:
                    os.makedirs(i)
                self.mount_from_storage(image, rootfs, upperdir, workdir)
                mounted_from_storage = True
            except (subprocess.CalledProcessError, ValueError):
                # Extract the image to a temp directory.
                self.extract(image, rootfs)

            # This part should be shared with install.
            values = {}
            if self.args.setvalues is not None:
                setvalues = SystemContainers._split_set_args(self.args.setvalues)
                for k, v in setvalues.items():
                    values[k] = v

            manifest_file = os.path.sep.join([rootfs, 'exports', "manifest.json"])
            manifest = None
            if os.path.exists(manifest_file):
                with open(manifest_file, "r") as f:
                    try:
                        manifest = json.loads(f.read())
                    except ValueError:
                        raise ValueError("Invalid manifest.json file in image: {}.".format(image))

            # if we got here, we know there is one image
            repo = self._get_ostree_repo()
            imgs = self._resolve_image(repo, image)
            _, rev = imgs[0]
            image_manifest = self._image_manifest(repo, rev)
            image_id = rev
            if image_manifest:
                image_manifest = json.loads(image_manifest)
                image_id = SystemContainers._get_image_id(repo, rev, image_manifest) or image_id

            self._amend_values(values, manifest, name, image, image_id, base_dir)

            # Check for config.json in exports
            destination_config = os.path.sep.join([base_dir, 'config.json'])
            template_config_file = os.path.sep.join([rootfs, 'exports', 'config.json.template'])
            template_tmpfiles = os.path.sep.join([rootfs, 'exports', 'tmpfiles.template'])
            # If there is a config.json, use it
            if os.path.exists(os.path.sep.join([rootfs, 'exports', 'config.json'])):
                shutil.copy(os.path.sep.join([rootfs, 'exports', 'config.json']),
                            destination_config)
            # Else, if we have a template, populate it
            elif os.path.exists(template_config_file):
                with open(template_config_file, 'r') as infile:
                    util.write_template(template_config_file, infile.read(), values, destination_config)
            # Otherwise, use a default one
            else:
                self._generate_default_oci_configuration(destination_config)

            # If we have a tmpfiles template, populate it
            if os.path.exists(template_tmpfiles):
                with open(template_tmpfiles, 'r') as infile:
                    tmp = os.path.sep.join([base_dir, 'tmpfiles.conf'])
                    util.write_template(template_tmpfiles, infile.read(), values, tmp)
                    self._systemd_tmpfiles("--create", tmp)
                    tmpfiles_destination = tmp

            # Get the start command for the system container
            (start_command, _, _, _) = self._generate_systemd_startstop_directives(name, unit_file_support_pidfile=False)
            # Move to the base directory to start the system container
            os.chdir(base_dir)
            # ... and run it. We use call() because the actual
            # run may be expected to fail.
            return util.call(start_command)
        finally:
            if tmpfiles_destination:
                try:
                    self._systemd_tmpfiles("--remove", tmpfiles_destination)
                except subprocess.CalledProcessError:
                    pass
            # Remove the temporary checkout
            if mounted_from_storage:
                util.call("umount %s" % rootfs)
            shutil.rmtree(base_dir)


    def _install(self, image, name):
        """
        Internal container install logic.
        """
        repo = self._get_ostree_repo()
        if not repo:
            raise ValueError("Cannot find a configured OSTree repo")

        if self.args.system and self.user:
            raise ValueError("Only root can use --system")

        accepted_system_package_values = ['auto', 'build', 'no', 'yes']
        if self.args.system_package not in accepted_system_package_values:
            raise ValueError("Invalid --system-package mode.  Accepted values: '%s'" % "', '".join(accepted_system_package_values))

        if self.get_checkout(name):
            util.write_out("%s already present" % (name))
            return

        image = self._pull_image_to_ostree(repo, image, False)
        if self.args.system_package == 'auto' and self.user:
            self.args.system_package = 'absent'

        if self.args.system_package in ['build'] and not self.args.system:
            raise ValueError("Only --system can generate rpms")

        values = {}
        if self.args.setvalues is not None:
            setvalues = SystemContainers._split_set_args(self.args.setvalues)
            for k, v in setvalues.items():
                values[k] = v

        if self.args.system_package == 'build':
            destination = self.build_rpm(repo, name, image, values, os.getcwd())
            if destination:
                util.write_out("Generated rpm %s" % destination)
            return False

        self._checkout(repo, name, image, 0, False, values=values, remote=self.args.remote, system_package=self.args.system_package)

    def _check_oci_configuration_file(self, conf_path, remote=None, include_all=False):
        with open(conf_path, 'r') as conf:
            try:
                configuration = json.loads(conf.read())
            except ValueError:
                raise ValueError("Invalid json in configuration file: {}.".format(conf_path))
        # empty file, nothing to do here
        if len(configuration) == 0:
            return []
        if not 'root' in configuration or \
           not 'readonly' in configuration['root'] or \
           not configuration['root']['readonly']:
            raise ValueError("Invalid configuration file.  Only readonly images are supported")
        if configuration['root']['path'] != 'rootfs' and not remote:
            raise ValueError("Invalid configuration file.  Path must be 'rootfs'")

        missing_source_paths = []
        # Ensure that the source path specified in bind/rbind exists
        if "mounts" in configuration:
            for mount in configuration["mounts"]:
                if not "type" in mount:
                    continue
                if "source" in mount and "bind" in mount["type"]:
                    source = mount["source"]
                    if include_all or not os.path.exists(source):
                        missing_source_paths.append(source)
        return missing_source_paths

    def _generate_default_oci_configuration(self, destination):
        conf_path = os.path.join(destination, "config.json")

        # If runc is not installed we are not able to generate the default configuration,
        # write an empty JSON file
        if not util.runc_available():
            with open(conf_path, 'w') as conf:
                conf.write('{}')
            return

        args = [self._get_oci_runtime(), 'spec']
        util.subp(args, cwd=destination)
        with open(conf_path, 'r') as conf:
            configuration = json.loads(conf.read())
        configuration['root']['readonly'] = True
        configuration['root']['path'] = 'rootfs'
        configuration['process']['terminal'] = False
        configuration['process']['args'] = ['run.sh']
        with open(conf_path, 'w') as conf:
            conf.write(json.dumps(configuration, indent=4))

    def _get_oci_runtime(self):
        if self.runtime:
            return self.runtime

        if self._runtime_from_info_file:
            return self._runtime_from_info_file

        if self.user:
            return util.BWRAP_OCI_PATH
        return util.RUNC_PATH

    def _generate_systemd_startstop_directives(self, name, pidfile=None, unit_file_support_pidfile=False):
        runtime = self._get_oci_runtime()
        return self._generate_systemd_runtime_startstop_directives(name, runtime, pidfile=pidfile, unit_file_support_pidfile=unit_file_support_pidfile)

    def _generate_systemd_runtime_startstop_directives(self, name, runtime, pidfile=None, unit_file_support_pidfile=False):
        runtime_has_pidfile = "--pid-file" in str(util.check_output([runtime, "run", "--help"], stderr=DEVNULL))
        systemd_cgroup = " --systemd-cgroup" if "--systemd-cgroup" in str(util.check_output([runtime, "--help"], stderr=DEVNULL)) else ""

        if unit_file_support_pidfile and runtime_has_pidfile:
            start = "{} {} run -d --pid-file {} '{}'".format(runtime, systemd_cgroup, pidfile, name)
            stoppost = "{} delete '{}'".format(runtime, name)
            return [start, "", "", stoppost]
        else:
            runc_commands = ["run", "kill"]
            return ["{}{} {} '{}'".format(runtime, systemd_cgroup, command, name) for command in runc_commands] + ["", ""]

    def _get_systemd_destination_files(self, name, prefix=None):
        if self.user:
            unitfileout = os.path.join(SYSTEMD_UNIT_FILES_DEST_USER, "%s.service" % name)
            tmpfilesout = os.path.join(SYSTEMD_TMPFILES_DEST_USER, "%s.conf" % name)
        else:
            if prefix:
                unitfileout = os.path.join(SYSTEMD_UNIT_FILES_DEST_PREFIX % prefix, "%s.service" % name)
                tmpfilesout = os.path.join(SYSTEMD_TMPFILES_DEST_PREFIX % prefix, "%s.conf" % name)
            else:
                unitfileout = os.path.join(SYSTEMD_UNIT_FILES_DEST, "%s.service" % name)
                tmpfilesout = os.path.join(SYSTEMD_TMPFILES_DEST, "%s.conf" % name)
        return unitfileout, tmpfilesout

    def _resolve_remote_path(self, remote_path):
        if not remote_path:
            return None

        real_path = os.path.realpath(remote_path)
        if not os.path.exists(real_path):
            raise ValueError("The container's rootfs is set to remote, but the remote rootfs does not exist")
        return real_path

    def _checkout(self, repo, name, img, deployment, upgrade, values=None, destination=None, extract_only=False, remote=None, prefix=None, installed_files_checksum=None, system_package='no'):
        destination = destination or os.path.join(self._get_system_checkout_path(), "{}.{}".format(name, deployment))
        unitfileout, tmpfilesout = self._get_systemd_destination_files(name, prefix)

        if not upgrade:
            for f in [unitfileout, tmpfilesout]:
                if os.path.exists(f):
                    raise ValueError("The file %s already exists." % f)

        try:
            return self._do_checkout(repo, name, img, upgrade, deployment, values, destination, unitfileout, tmpfilesout, extract_only, remote, prefix,
                                     installed_files_checksum=installed_files_checksum, system_package=system_package)
        except (GLib.Error, ValueError, OSError, subprocess.CalledProcessError, KeyboardInterrupt) as e:
            try:
                if not extract_only and not upgrade:
                    shutil.rmtree(destination)
            except OSError:
                pass
            try:
                if not extract_only and not upgrade:
                    os.unlink(unitfileout)
            except OSError:
                pass
            try:
                if not extract_only and not upgrade:
                    os.unlink(tmpfilesout)
            except OSError:
                pass
            raise e

    @staticmethod
    def _template_support_pidfile(template):
        return "$EXEC_STOPPOST" in template and "$PIDFILE" in template

    @staticmethod
    def _get_image_id(repo, rev, image_manifest):
        # Allow to override the image id read from the manifest so that
        # we can test atomic updates even though the image itself was not
        # changed.  This must be used only for tests.
        if os.environ.get("ATOMIC_OSTREE_TEST_FORCE_IMAGE_ID"):
            return os.environ.get("ATOMIC_OSTREE_TEST_FORCE_IMAGE_ID")

        digest = SystemContainers._get_commit_metadata(repo, rev, "docker.digest")
        if digest is not None:
            return SystemContainers._drop_sha256_prefix(digest)

        if 'Digest' in image_manifest:
            image_id = image_manifest['Digest']
        elif 'config' in image_manifest and 'digest' in image_manifest['config']:
            image_id = image_manifest['config']['digest']
        else:
            return None
        return SystemContainers._drop_sha256_prefix(image_id)

    # Accept both name and version Id, and return the ostree rev
    def _resolve_image(self, repo, img, allow_multiple=False):
        imagebranch = SystemContainers._get_ostree_image_branch(img)
        rev = repo.resolve_rev(imagebranch, True)[1]
        if rev:
            return [(imagebranch, rev)]

        # if we could not find an image with the specified name, check if it is the prefix
        # of an ID, and allow it only for tagged images.
        if not str.isalnum(str(img)):
            return None

        tagged_images = [i for i in self.get_system_images(get_all=True, repo=repo) if i['RepoTags']]
        matches = [i for i in tagged_images if i['ImageId'].startswith(img)]

        if len(matches) == 0:
            return None

        if len(matches) > 1 and not allow_multiple:
            # more than one match, error out
            raise ValueError("more images matching prefix `%s`" % img)

        # only one image, use it
        def get_image(i):
            repotag = i['RepoTags'][0]
            if repotag == '<none>':
                imagebranch = "%s%s" % (OSTREE_OCIIMAGE_PREFIX, i['Id'])
            else:
                imagebranch = "%s%s" % (OSTREE_OCIIMAGE_PREFIX, SystemContainers._encode_to_ostree_ref(repotag))
            return imagebranch, i['OSTree-rev']

        return [get_image(i) for i in matches]

    def _should_be_installed_rpm(self, exports):
        if os.path.exists("/run/ostree-booted"):
            return False
        for i in ["rpm.spec", "rpm.spec.template", "hostfs"]:
            if os.path.exists(os.path.join(exports, i)):
                return True
        return False


    @staticmethod
    def _get_all_capabilities():
        all_caps = util.get_all_known_process_capabilities()
        return ",\n".join(['"%s"' % i for i in all_caps]) + "\n"

    def _amend_values(self, values, manifest, name, image, image_id, destination, prefix=None, unit_file_support_pidfile=False):
        # When installing a new system container, set values in this order:
        #
        # 1) What comes from manifest.json, if present, as default value.
        # 2) What the user sets explictly as --set
        # 3) Values for DESTDIR and NAME
        if "RUN_DIRECTORY" not in values:
            if self.user:
                values["RUN_DIRECTORY"] = os.environ.get("XDG_RUNTIME_DIR", "/run/user/%s" % (os.getuid()))
            else:
                values["RUN_DIRECTORY"] = "/run"

        if "PIDFILE" not in values:
            values["PIDFILE"] = os.path.sep.join([values["RUN_DIRECTORY"], "container-{}.pid".format(name)])

        if "CONF_DIRECTORY" not in values:
            if self.user:
                values["CONF_DIRECTORY"] = os.path.join(HOME, ".config")
            else:
                values["CONF_DIRECTORY"] = "/etc"

        if "STATE_DIRECTORY" not in values:
            if self.user:
                values["STATE_DIRECTORY"] = os.path.join(HOME, ".data")
            else:
                values["STATE_DIRECTORY"] = "/var/lib"

        if "ALL_PROCESS_CAPABILITIES" not in values:
            values["ALL_PROCESS_CAPABILITIES"] = SystemContainers._get_all_capabilities()

        if 'RUNTIME' not in values:
            values["RUNTIME"] = self._get_oci_runtime()

        if 'ATOMIC' not in values:
            values["ATOMIC"] = os.path.abspath(__main__.__file__)

        if manifest is not None and "defaultValues" in manifest:
            for key, val in manifest["defaultValues"].items():
                if key not in values:
                    values[key] = val

        if "UUID" not in values:
            values["UUID"] = str(uuid.uuid4())
        values["DESTDIR"] = os.path.join("/", os.path.relpath(destination, prefix)) if prefix else destination
        values["NAME"] = name
        directives = self._generate_systemd_startstop_directives(name, pidfile=values["PIDFILE"], unit_file_support_pidfile=unit_file_support_pidfile)
        values["EXEC_START"], values["EXEC_STOP"], values["EXEC_STARTPRE"], values["EXEC_STOPPOST"] = directives
        values["HOST_UID"] = os.getuid()
        values["HOST_GID"] = os.getgid()
        values["IMAGE_NAME"] = image
        values["IMAGE_ID"] = image_id
        return values

    @staticmethod
    def _is_repo_on_the_same_filesystem(repo, destdir):
        """
        :param repo: Existing ostree repository.
        :type repo: str

        :param destdir: path to check.  It is created if it doesn't exist.  It must be
        writeable as we create a temporary file there.
        :type destdir: str
        """
        repo_stat = os.stat(repo)
        try:
            destdir_stat = os.stat(destdir)
        except OSError as e:
            if e.errno == errno.ENOENT:
                # The directory doesn't exist
                os.makedirs(destdir)
                destdir_stat = os.stat(destdir)
            else:
                raise e

        # Simple case: st_dev is different
        if repo_stat.st_dev != destdir_stat.st_dev:
            return False

        src_file = os.path.join(repo, "config")
        dest_file = os.path.join(destdir, "samefs-check-{}".format(os.getpid()))
        # Try to create a link, check if it fails with EXDEV
        try:
            os.link(src_file, dest_file)
        except OSError as e:
            if e.errno == errno.EXDEV:
                return False
            raise
        finally:
            try:
                os.unlink(dest_file)
            except OSError:
                pass
        return True

    @staticmethod
    def _are_same_file(a, b):
        a_stat = os.stat(a)
        b_stat = os.stat(b)
        return a_stat.st_dev == b_stat.st_dev and a_stat.st_ino == b_stat.st_ino

    def _canonicalize_location(self, destination):
        # Under Atomic, get the real deployment location if we're using the
        # system repo. It is needed to create the hard links.
        if self.get_ostree_repo_location() != '/ostree/repo':
            return destination
        try:
            sysroot = OSTree.Sysroot()
            sysroot.load()
            osname = sysroot.get_booted_deployment().get_osname()
            ostree_destination = os.path.realpath(
                os.path.join("/ostree/deploy/", osname, os.path.relpath(
                    destination, "/")))

            # Verify that content under the ostree destination shows up on destination.
            # If it does, we use the ostree destination.
            if not os.path.exists(os.path.dirname(destination)):
                os.makedirs(os.path.dirname(destination))
            if SystemContainers._are_same_file(os.path.dirname(destination), os.path.dirname(ostree_destination)):
                destination = ostree_destination

        except: #pylint: disable=bare-except
            pass
        return destination


    def _do_checkout(self, repo, name, img, upgrade, deployment, values, destination, unitfileout,
                     tmpfilesout, extract_only, remote, prefix=None, installed_files_checksum=None, system_package='no'):
        """
        Actually do the checkout.

        .. todo::

           This method should be simplified by breaking it up into smaller, reusable methods.

        :rtype: dict
        :raises: AttributeError,NameError, OSError, ValueError
        """
        if values is None:
            values = {}

        # Get the rev or raise out of the method
        try:
            _, rev = self._resolve_image(repo, img)[0]
        except (IndexError, TypeError):
            raise ValueError("Image {} not found".format(img))

        remote_path = self._resolve_remote_path(remote)

        if remote_path:
            remote_rootfs = os.path.sep.join([remote_path, "rootfs"])
            if os.path.exists(remote_rootfs):
                util.write_out("The remote rootfs for this container is set to be {}".format(remote_rootfs))
            elif os.path.exists(os.path.sep.join([remote, "usr"])):  # Assume that the user directly gave the location of the rootfs
                remote_rootfs = remote
                remote_path = os.path.dirname(remote_path)  # Use the parent directory as the "container location"
            else:
                raise ValueError("--remote was specified but the given location does not contain a rootfs")
            exports = os.path.join(remote_path, "rootfs/exports")
        else:
            exports = os.path.join(destination, "rootfs/exports")

        unitfile = os.path.sep.join([exports, "service.template"])
        tmpfiles = os.path.sep.join([exports, "tmpfiles.template"])

        util.write_out("Extracting to {}".format(destination))

        # upgrade will not restart the service if it was not already running
        was_service_active = self._is_service_active(name)

        if self.display:
            return values

        if extract_only:
            rootfs = destination
        elif remote_path:
            rootfs = os.path.join(remote_path, "rootfs")
        else:
            destination = self._canonicalize_location(destination)
            rootfs = os.path.join(destination, "rootfs")

        if remote_path:
            if not os.path.exists(destination):
                os.makedirs(destination)
        else:
            if not os.path.exists(rootfs):
                os.makedirs(rootfs)

        manifest = self._image_manifest(repo, rev)

        if not remote_path:
            rootfs_fd = None
            try:
                rootfs_fd = os.open(rootfs, os.O_DIRECTORY)
                if manifest is None:
                    self._checkout_layer(repo, rootfs_fd, rootfs, rev)
                else:
                    layers = SystemContainers.get_layers_from_manifest(json.loads(manifest))
                    for layer in layers:
                        rev_layer = repo.resolve_rev("%s%s" % (OSTREE_OCIIMAGE_PREFIX, layer.replace("sha256:", "")), True)[1]
                        if not rev_layer:
                            raise ValueError("Layer not found: %s.  Please pull the image again" % layer.replace("sha256:", ""))

                        self._checkout_layer(repo, rootfs_fd, rootfs, rev_layer)
                self._do_syncfs(rootfs, rootfs_fd)
            finally:
                if rootfs_fd:
                    os.close(rootfs_fd)

        if extract_only:
            return values

        if not os.path.exists(exports):
            util.write_out("""Warning: /exports directory not found.  Default config files will be generated.
Warning: You may want to modify `%s` before starting the service""" % os.path.join(destination, "config.json"))

        if system_package == 'auto':
            system_package = "yes" if self._should_be_installed_rpm(exports) else 'no'

        manifest_file = os.path.join(exports, "manifest.json")
        installed_files_template = []
        has_container_service = True
        rename_files = {}
        manifest = None
        if os.path.exists(manifest_file):
            with open(manifest_file, "r") as f:
                try:
                    manifest = json.loads(f.read())
                except ValueError:
                    raise ValueError("Invalid manifest.json file in image: {}.".format(img))
                if "installedFilesTemplate" in manifest:
                    installed_files_template = manifest["installedFilesTemplate"]
                if "renameFiles" in manifest:
                    rename_files = manifest["renameFiles"]
                if "noContainerService" in manifest and manifest["noContainerService"]:
                    has_container_service = False

        image_manifest = self._image_manifest(repo, rev)
        image_id = rev
        if image_manifest:
            image_manifest = json.loads(image_manifest)
            image_id = SystemContainers._get_image_id(repo, rev, image_manifest) or image_id

        if os.path.exists(unitfile):
            with open(unitfile, 'r') as infile:
                systemd_template = infile.read()
        else:
            systemd_template = SYSTEMD_UNIT_FILE_DEFAULT_TEMPLATE

        values = self._amend_values(values, manifest, name, img, image_id, destination, prefix, unit_file_support_pidfile=SystemContainers._template_support_pidfile(systemd_template))

        src = os.path.join(exports, "config.json")
        destination_path = os.path.join(destination, "config.json")
        if os.path.exists(src):
            shutil.copyfile(src, destination_path)
        elif os.path.exists(src + ".template"):
            with open(src + ".template", 'r') as infile:
                util.write_template(src + ".template", infile.read(), values, destination_path)
        else:
            self._generate_default_oci_configuration(destination)

        if remote_path:
            with open(destination_path, 'r') as config_file:
                try:
                    config = json.loads(config_file.read())
                except ValueError:
                    raise ValueError("Invalid config.json file in given remote location: {}.".format(destination_path))
                config['root']['path'] = remote_rootfs
            with open(destination_path, 'w') as config_file:
                config_file.write(json.dumps(config, indent=4))
            # create a symlink to the real rootfs, so that it is possible
            # to access the rootfs in the same way as in the not --remote case.
            os.symlink(remote_rootfs, os.path.join(destination, "rootfs"))

        # When upgrading, stop the service and remove previously installed
        # tmpfiles, before restarting the service.
        if has_container_service and upgrade:
            if was_service_active:
                self._systemctl_command("stop", name)
            if os.path.exists(tmpfilesout):
                try:
                    self._systemd_tmpfiles("--remove", tmpfilesout)
                except subprocess.CalledProcessError:
                    pass

        # rename_files may contain variables that need to be replaced.
        if rename_files:
            for k, v in rename_files.items():
                template = Template(v)
                try:
                    new_v = template.substitute(values)
                except KeyError as e:
                    raise ValueError("The template file 'manifest.json' still contains an unreplaced value for: '%s'" % \
                                     (str(e)))
                rename_files[k] = new_v

        missing_bind_paths = self._check_oci_configuration_file(destination_path, remote_path, False)

        # let's check if we can generate an rpm from the /exports directory
        rpm_file = rpm_installed = None
        if system_package == 'yes':
            img_obj = self.inspect_system_image(img)
            image_id = img_obj["ImageId"]
            labels = {k.lower() : v for k, v in img_obj.get('Labels', {}).items()}
            (rpm_installed, rpm_file, _) = RPMHostInstall.generate_rpm(name, image_id, labels, exports, destination, values=values, installed_files_template=installed_files_template, rename_files=rename_files, defaultversion=deployment)
        if rpm_installed or system_package == 'absent':
            new_installed_files_checksum = {}
        else:
            new_installed_files_checksum = RPMHostInstall.rm_add_files_to_host(installed_files_checksum, exports, prefix or "/", files_template=installed_files_template, values=values, rename_files=rename_files)

        new_installed_files = list(new_installed_files_checksum.keys())
        try:
            with open(os.path.join(destination, "info"), 'w') as info_file:
                info = {"image" : img,
                        "revision" : image_id,
                        "ostree-commit": rev,
                        'created' : calendar.timegm(time.gmtime()),
                        "values" : values,
                        "has-container-service" : has_container_service,
                        "installed-files": new_installed_files,
                        "installed-files-checksum": new_installed_files_checksum,
                        "installed-files-template": installed_files_template,
                        "rename-installed-files" : rename_files,
                        "rpm-installed" : rpm_installed,
                        "system-package" : system_package,
                        "remote" : remote,
                        "runtime" : self._get_oci_runtime()}
                info_file.write(json.dumps(info, indent=4))
                info_file.write("\n")
        except (NameError, AttributeError, OSError) as e:
            for i in new_installed_files:
                os.remove(os.path.join(prefix or "/", os.path.relpath(i, "/")))
            raise e

        if os.path.exists(tmpfiles):
            with open(tmpfiles, 'r') as infile:
                tmpfiles_template = infile.read()
        else:
            tmpfiles_template = SystemContainers._generate_tmpfiles_data(missing_bind_paths)

        if has_container_service:
            util.write_template(unitfile, systemd_template, values, unitfileout)
            shutil.copyfile(unitfileout, os.path.join(prefix or "/", destination, "%s.service" % name))
        if (tmpfiles_template):
            util.write_template(unitfile, tmpfiles_template, values, tmpfilesout)
            shutil.copyfile(tmpfilesout, os.path.join(prefix or "/", destination, "tmpfiles-%s.conf" % name))

        if not prefix:
            sym = os.path.join(self._get_system_checkout_path(), name)
            if os.path.exists(sym):
                os.unlink(sym)
            os.symlink(destination, sym)

        # if there is no container service, delete the checked out files.  At this point files copied to the host
        # are already handled.
        if not has_container_service:
            if not remote_path:
                shutil.rmtree(os.path.join(destination, "rootfs"))
            return values

        if prefix:
            return values

        sym = os.path.join(self._get_system_checkout_path(), name)
        if os.path.exists(sym):
            os.unlink(sym)
        os.symlink(destination, sym)

        try:
            if rpm_installed:
                RPMHostInstall.install_rpm(rpm_file)
            else:
                for installed_file in new_installed_files:
                    util.write_out("Created file {}".format(installed_file))

            self._systemctl_command("daemon-reload")
            if (tmpfiles_template):
                self._systemd_tmpfiles("--create", tmpfilesout)

            if not upgrade:
                self._systemctl_command("enable", name)
            elif was_service_active:
                self._systemctl_command("start", name)
        except (subprocess.CalledProcessError, KeyboardInterrupt):
            if rpm_installed:
                RPMHostInstall.uninstall_rpm(rpm_installed)
            for installed_file in new_installed_files:
                os.unlink(installed_file)
            os.unlink(sym)
            raise

        return values

    def _get_preinstalled_containers_path(self):
        return ATOMIC_USR

    def _get_system_checkout_path(self):
        if os.environ.get("ATOMIC_OSTREE_CHECKOUT_PATH"):
            return os.environ.get("ATOMIC_OSTREE_CHECKOUT_PATH")
        if self.get_atomic_config_item(["checkout_path"]):
            return self.get_atomic_config_item(["checkout_path"])
        if self.user:
            return ATOMIC_VAR_USER
        else:
            return ATOMIC_VAR

    def get_ostree_repo_location(self):
        """
        Returns the ostree repo location on disk.

        :returns: Path to the ostree repository.
        :rtype: str
        """
        if self._repo_location is None:
            self._repo_location = self._find_ostree_repo_location()
        return self._repo_location

    def _find_ostree_repo_location(self):
        location = os.environ.get("ATOMIC_OSTREE_REPO")
        if location is not None:
            return location

        if self.user:
            return os.path.join(HOME, ".containers/repo")

        if self.get_atomic_config_item(["ostree_repository"]) is not None:
            return self.get_atomic_config_item(["ostree_repository"])

        # If the checkout directory and the OSTree storage are not on the same
        # file system, then we cannot use the system repository, as it would not
        # support hard links checkouts.
        checkouts = self._get_system_checkout_path()
        if not os.path.exists(checkouts):
            os.makedirs(checkouts)

        storage_path = self.get_storage_path(skip_canonicalize=True)

        # Use /ostree/repo if it already exists and it is on the same filesystem
        # as the checkout directory.
        if os.path.exists("/ostree/repo/config"):
            if SystemContainers._is_repo_on_the_same_filesystem("/ostree/repo", storage_path):
                return "/ostree/repo"
        return os.path.join(storage_path, "ostree")

    def _get_ostree_repo(self):
        if not OSTREE_PRESENT:
            return None

        repo_location = self.get_ostree_repo_location()
        repo = OSTree.Repo.new(Gio.File.new_for_path(repo_location))

        # If the repository doesn't exist at the specified location, create it
        if not os.path.exists(os.path.join(repo_location, "config")):
            os.makedirs(repo_location)
            if self.user:
                repo.create(OSTree.RepoMode.BARE_USER)
            else:
                repo.create(OSTree.RepoMode.BARE)

        repo.open(None)
        return repo

    def version(self, image):
        """
        Returns information on provided image in ostree. Same as foo = [inspect_system_image(image)].

        :rtype: list or None
        """
        image_inspect = self.inspect_system_image(image)
        if image_inspect:
            return [image_inspect]
        return None

    def update_container(self, name, setvalues=None, rebase=None):
        """
        Updates a container to a new version.

        :param name: Name of the container to update.
        :type name: str
        :param setvalues: List of key=value items to set in the updated container.
        :type setvalues: list or None
        :param rebase: Name of the image to rebase or None to sense from info file.
        :type rebase: str or None
        """
        if self._is_preinstalled_container(name):
            raise ValueError("Cannot update a preinstalled container")

        repo = self._get_ostree_repo()
        if not repo:
            raise ValueError("Cannot find a configured OSTree repo")

        path = os.path.join(self._get_system_checkout_path(), name)
        with open(os.path.join(path, "info"), 'r') as info_file:
            info = json.loads(info_file.read())
            self.args.remote = info['remote']
            if self.args.remote:
                util.write_out("Updating a container with a remote rootfs. Only changes to config will be applied.")

        next_deployment = 0
        if os.path.realpath(path).endswith(".0"):
            next_deployment = 1

        with open(os.path.join(self._get_system_checkout_path(), name, "info"), "r") as info_file:
            info = json.loads(info_file.read())

        image = rebase or info["image"]
        values = info["values"]
        revision = info["revision"] if "revision" in info else None
        installed_files_checksum = info["installed-files-checksum"] if "installed-files-checksum" in info else None
        if installed_files_checksum is None:
            installed_files = info["installed-files"] if "installed-files" in info else None
            installed_files_checksum = {k : "" for k in installed_files}
        rpm_installed = info["rpm-installed"] if "rpm-installed" in info else None
        system_package = info["system-package"] if "system-package" in info else None
        runtime = info["runtime"] if "runtime" in info else None

        # Check if the image id or the configuration for the container has
        # changed before upgrading it.
        revision_changed = True
        if revision:
            image_inspect = self.inspect_system_image(image)
            if image_inspect:
                if image_inspect['ImageId'] == revision:
                    revision_changed = False

        # Override values with anything coming from setvalues and while at it
        # check if anything was changed.
        values_changed = False
        if setvalues:
            for k, v in SystemContainers._split_set_args(setvalues).items():
                old = values.get(k)
                values[k] = v
                if old != v:
                    values_changed = True

        if not revision_changed and not values_changed:
            # Nothing to do
            util.write_out("Latest version already installed.")
            return

        if runtime is not None:
            self._runtime_from_info_file = runtime
        if system_package is None:
            system_package = 'yes' if rpm_installed else 'no'
        self._checkout(repo, name, image, next_deployment, True, values, remote=self.args.remote,
                       installed_files_checksum=installed_files_checksum, system_package=system_package)
        return

    def rollback(self, name):
        """
        Rolls back a container to a previous version. If the service is rolledback by running
        the service will be stopped, rolled back, and then started.

        :param name: The container to rollback.
        :type name: str
        """
        path = os.path.join(self._get_system_checkout_path(), name)
        destination = "%s.%d" % (path, (1 if os.path.realpath(path).endswith(".0") else 0))
        if not os.path.exists(destination):
            raise ValueError("Error: Cannot find a previous deployment to rollback located at %s" % destination)

        installed_files = None
        rename_files = None
        with open(os.path.join(self._get_system_checkout_path(), name, "info"), "r") as info_file:
            info = json.loads(info_file.read())
            rpm_installed = info["rpm-installed"] if "rpm-installed" in info else None
            installed_files_checksum = info["installed-files-checksum"] if "installed-files-checksum" in info else None
            if installed_files_checksum is None:
                installed_files = info["installed-files"] if "installed-files" in info else None
                installed_files_checksum = {k : "" for k in installed_files}
            installed_files_template = info["installed-files-template"] if "installed-files-template" in info and rpm_installed is None else None
            has_container_service = info["has-container-service"] if "has-container-service" in info else True
            rename_files = info["rename-installed-files"] if "rename-installed-files" in info else None

        was_service_active = has_container_service and self._is_service_active(name)
        unitfileout, tmpfilesout = self._get_systemd_destination_files(name)
        unitfile = os.path.join(destination, "%s.service" % name)
        tmpfiles = os.path.join(destination, "tmpfiles-%s.conf" % name)

        if not os.path.exists(unitfile):
            raise ValueError("Error: Cannot find systemd service file for previous version. "
                             "The previous checkout at %s may be corrupted." % destination)

        util.write_out("Rolling back container {} to the checkout at {}".format(name, destination))

        if was_service_active:
            self._systemctl_command("stop", name)

        if os.path.exists(tmpfilesout):
            try:
                self._systemd_tmpfiles("--remove", tmpfilesout)
            except subprocess.CalledProcessError:
                pass
            os.unlink(tmpfilesout)

        if os.path.exists(unitfileout):
            os.unlink(unitfileout)

        shutil.copyfile(unitfile, unitfileout)
        if (os.path.exists(tmpfiles)):
            shutil.copyfile(tmpfiles, tmpfilesout)

        if installed_files_checksum:
            RPMHostInstall.rm_add_files_to_host(installed_files_checksum, os.path.join(destination, "rootfs/exports"), files_template=installed_files_template, rename_files=rename_files)

        os.unlink(path)
        os.symlink(destination, path)

        # reinstall the previous rpm if any.
        rpm_installed = None
        with open(os.path.join(self._get_system_checkout_path(), name, "info"), "r") as info_file:
            info = json.loads(info_file.read())
            rpm_installed = info["rpm-installed"] if "rpm-installed" in info else "container.rpm"

        if rpm_installed:
            RPMHostInstall.install_rpm(os.path.join(self._get_system_checkout_path(), name, rpm_installed))

        if has_container_service:
            self._systemctl_command("daemon-reload")
        if (os.path.exists(tmpfiles)):
            self._systemd_tmpfiles("--create", tmpfilesout)

        if was_service_active:
            self._systemctl_command("start", name)

    def get_container_runtime_info(self, container):
        """
        Returns the status of a container in terms of it's service.

        :name container: The name of the container to query.
        :type container: str
        :returns: The status of the service.
        :rtype: dict
        """
        info_path = os.path.join(self._get_system_checkout_path(), container, "info")
        if not os.path.exists(info_path):
            info_path = os.path.join(self._get_preinstalled_containers_path(), container, "info")

        with open(info_path, "r") as info_file:
            info = json.loads(info_file.read())
            has_container_service = info["has-container-service"] if "has-container-service" in info else True

        if not has_container_service:
            return {'status' : "no service"}
        if self._is_service_active(container):
            return {'status' : "running"}
        elif self._is_service_failed(container):
            return {'status' : "failed"}
        else:
            # The container is newly created or stopped, and can be started with 'systemctl start'
            return {'status' : "inactive"}

    def _get_containers_at(self, checkouts, are_preinstalled, containers=None):
        if not checkouts or not os.path.exists(checkouts):
            return []
        ret = []
        if containers is None:
            containers = os.listdir(checkouts)
        for x in containers:
            if x[0] == ".":
                continue
            fullpath = os.path.join(checkouts, x)
            if not os.path.exists(fullpath):
                continue
            if fullpath.endswith(".0") or fullpath.endswith(".1"):
                continue

            with open(os.path.join(fullpath, "info"), "r") as info_file:
                info = json.load(info_file)
                revision = info["revision"] if "revision" in info else ""
                created = info["created"] if "created" in info else 0
                image = info["image"] if "image" in info else ""
                runtime = info["runtime"] if "runtime" in info else self._get_oci_runtime()

            command = ""
            config_json = os.path.join(fullpath, "config.json")
            if os.path.exists(config_json):
                with open(config_json, "r") as config_file:
                    config = json.load(config_file)
                    command = u' '.join(config["process"]["args"])

            container = {'Image' : image, 'ImageID' : revision, 'Id' : x, 'Created' : created, 'Names' : [x],
                         'Command' : command, 'Type' : 'system', 'Runtime' : runtime, "Preinstalled" : are_preinstalled}
            ret.append(container)
        return ret

    def get_containers(self, containers=None):
        """
        Returns containers on the system.

        :param containers: A specific list of containers to look for, or None for all.
        :type containers: list or None
        :returns: List of container information in dictionary format.
        :rtype: list
        """
        checkouts = self._get_system_checkout_path()
        preinstalled = self._get_preinstalled_containers_path()
        return self._get_containers_at(checkouts, False, containers) + self._get_containers_at(preinstalled, True, containers)

    def get_template_variables(self, image):
        """
        Returns all template variables for a specific image.

        :param image: The name of the image to get template variables.
        :type image: str
        :returns: Variables with defaults and variables that must be set
        :rtype: tuple
        """
        repo = self._get_ostree_repo()
        imgs = self._resolve_image(repo, image)
        if not imgs:
            return None, None
        _, commit_rev = imgs[0]
        manifest = self._image_manifest(repo, commit_rev)
        layers = SystemContainers.get_layers_from_manifest(json.loads(manifest))
        templates = {}
        manifest_template = None
        for i in layers:
            layer = i.replace("sha256:", "")
            commit = repo.read_commit(repo.resolve_rev("%s%s" % (OSTREE_OCIIMAGE_PREFIX, layer), True)[1])[1]
            if not commit:
                raise ValueError("Layer not found: %s.  Please pull the image again" % layer.replace("sha256:", ""))
            exports = commit.get_root().get_child("exports")
            if not exports.query_exists():
                continue

            children = exports.enumerate_children("", Gio.FileQueryInfoFlags.NONE, None)
            for child in reversed(list(children)):
                name = child.get_name()
                if name == "manifest.json":
                    manifest_template = exports.get_child(name).read()

                if name.endswith(".template"):
                    if name.startswith(".wh"):
                        name = name[4:]
                        templates.pop(name, None)
                    else:
                        templates[name] = exports.get_child(name).read()

        variables = {}
        for v in templates.values():
            fd = v.get_fd()
            with os.fdopen(fd) as f:
                data = f.read()
                template = Template(data)
                for variable in ["".join(x) for x in template.pattern.findall(data)]: # pylint: disable=no-member
                    if variable not in TEMPLATE_FORCED_VARIABLES:
                        variables[variable] = variable

        variables_with_default = {}
        if manifest_template:
            fd = manifest_template.get_fd()
            with os.fdopen(fd) as f:
                try:
                    data = json.loads(f.read())
                except ValueError:
                    raise ValueError("Invalid manifest.json file in image: {}.".format(image))
                for variable in data['defaultValues']:
                    variables_with_default[variable] = data['defaultValues'][variable]

        # Also include variables that are set by the OS
        # but can be overriden by --set
        for variable in TEMPLATE_OVERRIDABLE_VARIABLES:
            variables_with_default[variable] = "{SET_BY_OS}"

        variables_to_set = {}
        for variable in variables:
            if variable not in variables_with_default:
                variables_to_set[variable] = "{DEF_VALUE}"

        return variables_with_default, variables_to_set

    def delete_image(self, image):
        """
        Deletes a specific image.

        :param image: The name of the image to delete.
        :type image: str
        """
        repo = self._get_ostree_repo()
        if not repo:
            return
        imgs = self._resolve_image(repo, image, allow_multiple=True)
        if not imgs:
            return
        for imagebranch, _ in imgs:
            ref = OSTree.parse_refspec(imagebranch)
            repo.set_ref_immediate(ref[1], ref[2], None)

    def inspect_system_image(self, image):
        """
        Returns information on a specific image.

        :param image: The name of the image to inspect.
        :returns: Information on a given image.
        :rtype: dict or None
        """
        repo = self._get_ostree_repo()
        if not repo:
            return None
        return self._inspect_system_branch(repo, image)

    def _inspect_system_branch(self, repo, imagebranch):
        if imagebranch.startswith(OSTREE_OCIIMAGE_PREFIX):
            commit_rev = repo.resolve_rev(imagebranch, False)[1]
            if not commit_rev:
                raise ValueError("Layer not found: %s.  Please pull the image again" % imagebranch.replace("sha256:", ""))
        else:
            imgs = self._resolve_image(repo, imagebranch, allow_multiple=True)
            if imgs is None:
                raise ValueError("Image %s not found" % imagebranch)
            _, commit_rev = imgs[0]
        commit = repo.load_commit(commit_rev)[1]

        timestamp = OSTree.commit_get_timestamp(commit)
        branch_id = SystemContainers._decode_from_ostree_ref(imagebranch.replace(OSTREE_OCIIMAGE_PREFIX, ""))

        image_id = commit_rev
        id_ = None

        if len(branch_id) == 64:
            image_id = branch_id
            tag = "<none>"
        elif '@sha256:' in branch_id:
            id_ = branch_id
            tags = branch_id.rsplit('@sha256:', 1)
            tag = ":".join(tags)
        else:
            tag = ":".join(branch_id.rsplit(':', 1))

        labels = {}
        manifest = self._image_manifest(repo, commit_rev)
        virtual_size = None
        if manifest:
            manifest = json.loads(manifest)
            virtual_size = self._get_virtual_size(repo, manifest)
            if 'Labels' in manifest:
                labels = manifest['Labels']
            image_id = SystemContainers._get_image_id(repo, commit_rev, manifest) or image_id

        if self.user:
            image_type = "user"
        else:
            image_type = "system"

        return {'Id' : id_ or image_id, 'Version' : tag, 'ImageId' : image_id, 'RepoTags' : [tag], 'Names' : [],
                'Created': timestamp, 'ImageType' : image_type, 'Labels' : labels, 'OSTree-rev' : commit_rev,
                'VirtualSize': virtual_size}

    def _get_virtual_size(self, repo, manifest):
        total_size = 0
        for i in SystemContainers.get_layers_from_manifest(manifest):
            layer = i.replace("sha256:", "")
            rev = repo.resolve_rev("%s%s" % (OSTREE_OCIIMAGE_PREFIX, layer), True)[1]
            if not rev:
                return None

            size = self._get_commit_metadata(repo, rev, 'docker.size')
            if size == None:
                return None
            total_size += int(size)
        return total_size

    def get_system_images(self, get_all=False, repo=None):
        """
        Returns a list of system images.

        :param get_all: If all images should be returned.
        :type get_all: bool
        :param repo: The repo to look at or None for default
        :type repo: str or None
        :returns: A list of image data structures.
        :rtype: list
        """
        if repo is None:
            repo = self._get_ostree_repo()
            if repo is None:
                return []
        revs = [x for x in repo.list_refs()[1] if x.startswith(OSTREE_OCIIMAGE_PREFIX) \
                and (get_all or len(x) != len(OSTREE_OCIIMAGE_PREFIX) + 64)]

        return [self._inspect_system_branch(repo, x) for x in revs]

    def _is_service_active(self, name):
        try:
            ret = self._systemctl_command("is-active", name, quiet=True)
            return ret and ret.replace("\n", "") == "active"
        except subprocess.CalledProcessError:
            return False

    def _is_service_failed(self, name):
        try:
            is_failed = self._systemctl_command("is-failed", name, quiet=True).replace("\n", "")
        except subprocess.CalledProcessError as e:
            is_failed = e.output.decode('utf-8')
            if is_failed.replace("\n", "") != "inactive":
                return True

        if is_failed == "failed":
            return True
        elif is_failed == "active":
            return False
        else:
            # in case of "inactive", could be a stopped container or failed process
            try:
                status = self._systemctl_command("status", name, quiet=True)
            except subprocess.CalledProcessError as e:
                status = e.output.decode('utf-8')
            if 'FAILURE' in status:
                return True
            else:
                return False

    def start_service(self, name):
        """
        Starts a system container service.

        :param name: The name of the system container to start.
        :type name: str
        """
        try:
            self._systemctl_command("start", name)
        except subprocess.CalledProcessError as e:
            raise ValueError(e.output)

    def stop_service(self, name):
        """
        Stops a system container service.

        :param name: The name of the system container to stop.
        :type name: str
        """
        try:
            self._systemctl_command("stop", name)
        except subprocess.CalledProcessError as e:
            raise ValueError(e.output)

    def _systemd_tmpfiles(self, command, name):
        cmd = ["systemd-tmpfiles"] + [command, name]
        util.write_out(" ".join(cmd))
        if not self.display:
            util.check_call(cmd)

    def _systemctl_command(self, command, name=None, now=False, quiet=False):
        cmd = ["systemctl"]
        if self.user:
            cmd.append("--user")
        cmd.append(command)
        if now:
            cmd.append("--now")
        if name:
            cmd.append(name)
        if not quiet:
            util.write_out(" ".join(cmd))
        if not self.display:
            return util.check_output(cmd, stderr=DEVNULL).decode('utf-8')
        return None

    def get_checkout(self, name):
        """
        Returns the checkout location of a system container.

        :param name: The name of the system container to get the checkout path for.
        :type name: str
        :returns: The path to the checkout or None
        :rtype: str or None
        """
        if len(name) == 0:
            raise ValueError("Invalid container name")
        path = os.path.join(self._get_system_checkout_path(), name)
        if os.path.exists(path):
            return path

        path = os.path.join(self._get_preinstalled_containers_path(), name)
        if os.path.exists(path):
            return path

        return None

    def _is_preinstalled_container(self, name):
        path = os.path.join(self._get_system_checkout_path(), name)
        if os.path.exists(path):
            return False

        path = os.path.join(self._get_preinstalled_containers_path(), name)
        return os.path.exists(path)

    def uninstall(self, name):
        """
        Uninstalls an installed system container. If RPM features were used the rpm is removed as well.

        :param name: The name of the system container to uninstall.
        :type name: str
        """
        if self._is_preinstalled_container(name):
            RPMHostInstall.uninstall_rpm("%s-%s" % (RPM_NAME_PREFIX, name))
            return

        if not os.path.exists(os.path.join(self._get_system_checkout_path(), name)):
            return

        with open(os.path.join(self._get_system_checkout_path(), name, "info"), "r") as info_file:
            info = json.loads(info_file.read())
            has_container_service = info["has-container-service"] if "has-container-service" in info else True
            rpm_installed = info["rpm-installed"] if "rpm-installed" in info else None

        unitfileout, tmpfilesout = self._get_systemd_destination_files(name)
        if has_container_service:
            try:
                self._systemctl_command("disable", name, now=True)
            except subprocess.CalledProcessError:
                pass

        # rm the systemd service file as first step, so we are sure the service cannot be started again
        # once we start deleting files.
        if os.path.exists(unitfileout):
            os.unlink(unitfileout)

        try:
            self._systemctl_command("daemon-reload")
        except subprocess.CalledProcessError:
            pass

        if os.path.exists(tmpfilesout):
            try:
                self._systemd_tmpfiles("--remove", tmpfilesout)
            except subprocess.CalledProcessError:
                pass
            os.unlink(tmpfilesout)

        checkout = self._get_system_checkout_path()
        installed_files = None
        with open(os.path.join(checkout, name,  "info"), 'r') as info_file:
            info = json.loads(info_file.read())
            installed_files_checksum = info["installed-files-checksum"] if "installed-files-checksum" in info else None
            installed_files = info["installed-files"] if "installed-files" in info else []
            if installed_files_checksum == None:
                installed_files_checksum = {k: "" for k in installed_files}
        if installed_files_checksum:
            RPMHostInstall.rm_add_files_to_host(installed_files_checksum, None)

        if rpm_installed:
            try:
                RPMHostInstall.uninstall_rpm(rpm_installed.replace(".rpm", ""))
            except subprocess.CalledProcessError:
                pass

        # Until the symlink and the deployment are in place, we can attempt the uninstall again.
        # So treat as fatal each failure that happens before we rm the symlink.
        symlink_path = os.path.join(checkout, name)
        if os.path.lexists(symlink_path):
            os.unlink(symlink_path)

        # Nothing more left, rm -rf the deployments, if this fails "images prune" will take care
        # of it.
        for deploy in ["0", "1"]:
            deploy_path = os.path.join(checkout, "{}.{}".format(name, deploy))
            if os.path.exists(deploy_path):
                shutil.rmtree(deploy_path)

    def prune_ostree_images(self):
        """
        Removes unused images in ostree.
        """
        repo = self._get_ostree_repo()
        if not repo:
            return
        refs = {}
        app_refs = []

        # Check for dangling checkouts that couldn't be deleted on uninstall.
        checkouts = self._get_system_checkout_path()
        for i in os.listdir(checkouts):
            if i[0] == '.':
                continue
            if i[-2:] not in [".0", ".1"]:
                continue

            container = i[:-2]

            # Check if the container is still installed.
            if os.path.lexists(os.path.join(checkouts, container)):
                continue

            fullpath = os.path.join(checkouts, i)
            if not os.path.isdir(fullpath):
                continue

            # If we got here, the deployment is not related to an installed
            # container, so delete it.
            try:
                shutil.rmtree(fullpath)
            except OSError:
                util.write_err("Could not remove directory {}".format(fullpath))

        for i in repo.list_refs()[1]:
            if i.startswith(OSTREE_OCIIMAGE_PREFIX):
                if len(i) == len(OSTREE_OCIIMAGE_PREFIX) + 64:
                    refs[i] = False
                else:
                    invalid_encoding = False
                    for c in i.replace(OSTREE_OCIIMAGE_PREFIX, ""):
                        if not str.isalnum(str(c)) and c not in '.-_':
                            invalid_encoding = True
                            break
                    if invalid_encoding:
                        refs[i] = False
                    else:
                        app_refs.append(i)

        def visit(rev):
            commit_rev = repo.resolve_rev(rev, True)[1]
            if not commit_rev:
                raise ValueError("Layer not found: %s.  Please pull the image again" % rev.replace("sha256:", ""))
            manifest = self._image_manifest(repo, commit_rev)
            if not manifest:
                return
            for layer in SystemContainers.get_layers_from_manifest(json.loads(manifest)):
                refs[OSTREE_OCIIMAGE_PREFIX + layer.replace("sha256:", "")] = True

        for app in app_refs:
            visit(app)

        for k, v in refs.items():
            if not v:
                ref = OSTree.parse_refspec(k)
                util.write_out("Deleting %s" % k)
                repo.set_ref_immediate(ref[1], ref[2], None)
        repo.prune(OSTree.RepoPruneFlags.REFS_ONLY, -1)
        self._prune_storage(repo)


    @staticmethod
    def get_default_system_name(image):
        """
        Returns the system name of a given image.

        :param image: The name of the image to inspect.
        :type image: str
        :returns: The system name of the image (name-tag).
        :rtype: str
        """
        if '@sha256:' in image:
            image = image.split('@sha256:')[0]
        image = image.replace("oci:", "", 1).replace("docker:", "", 1)
        _, image, tag = SystemContainers._parse_imagename(image)
        name = image.split("/")[-1]
        if tag != "latest":
            name = "%s-%s" % (name, tag)

        return name

    @staticmethod
    def _parse_imagename(imagename):
        sep = imagename.find("/")
        reg, image = imagename[:sep], imagename[sep + 1:]
        if '.' not in reg:
            # if the registry doesn't look like a domain, consider it as the
            # image prefix
            reg = ""
            image = imagename
        sep = image.find(":")
        if sep > 0:
            return reg, image[:sep], image[sep + 1:]
        else:
            return reg, image, "latest"

    def _get_skopeo_args(self, image, full_resolution=False):
        """
        Finds the real image name and if the registry is secure or not.

        :param image: The image as given by the user.
        :type image: str
        :param full_resolution: If the image should be remotely resolved
        :type full_resolution: bool
        :returns: If the registry is secure and the real image name
        :rtype: tuple(bool, str)
        """
        # Remove prefixes which we use to map to skopeo args
        real_image = util.remove_skopeo_prefixes(image)

        if full_resolution:
            try:
                with AtomicDocker() as client:
                    real_image = util.find_remote_image(client, real_image) or real_image
            except NoDockerDaemon:
                pass

        return "http:" in image, real_image

    def _convert_to_skopeo(self, image):
        insecure, image = self._get_skopeo_args(image, full_resolution=True)

        if insecure:
            return ["--insecure"], "docker://" + image
        else:
            return None, "docker://" + image

    def _skopeo_get_manifest(self, image):
        args, img = self._convert_to_skopeo(image)
        return util.skopeo_inspect(img, args)

    def _skopeo_get_layers(self, image, layers):
        _, img = self._convert_to_skopeo(image)
        return util.skopeo_layers(img, [], layers)

    def _image_manifest(self, repo, rev):
        return SystemContainers._get_commit_metadata(repo, rev, "docker.manifest")

    def get_manifest(self, image, remote=False):
        """
        Returns the manifest for an image.

        :param image: The name of the image to inspect.
        :type image: str
        :param remote: If the image is remote.
        :type remote: bool
        :returns: The manifest structure or None
        :rtype: dict or None
        """
        repo = self._get_ostree_repo()
        if not repo:
            return None

        if remote:
            return self._skopeo_get_manifest(image)

        imagebranch = SystemContainers._get_ostree_image_branch(image)
        commit_rev = repo.resolve_rev(imagebranch, True)
        if not commit_rev:
            raise ValueError("Layer not found: %s.  Please pull the image again" % imagebranch.replace("sha256:", ""))
        if not commit_rev[1]:
            return None
        return self._image_manifest(repo, commit_rev[1])

    @staticmethod
    def get_layers_from_manifest(manifest):
        """
        Returns the layers in a manifest.

        :param manifest: The manifest to get the layers from
        :type manifest: dict
        :returns: List of layers
        :rtype: list
        """
        if isinstance(manifest, str):
            manifest = json.loads(manifest)

        fs_layers = manifest.get("fsLayers")
        if fs_layers:
            layers = list(i["blobSum"] for i in fs_layers)
            layers.reverse()
        elif "layers" in manifest:
            layers = [x['digest'] for x in manifest.get("layers")]
        else:
            layers = manifest.get("Layers")
        return layers

    def _import_layers_into_ostree(self, repo, imagebranch, manifest, layers):
        def get_directory_size(path):
            size = 0
            seen = {}
            for root, _, files in os.walk(path):
                for f in files:
                    s = os.lstat(os.path.join(root, f))
                    key = "%s-%s" % (s.st_dev, s.st_ino)
                    if key not in seen:
                        seen[key] = key
                        size += s.st_size
            return GLib.Variant('s', str(size))

        repo.prepare_transaction()
        for layer, tar in layers.items():
            mtree = OSTree.MutableTree()
            def filter_func(*args):
                info = args[2]

                if info.get_file_type() == Gio.FileType.SPECIAL:
                    return OSTree.RepoCommitFilterResult.SKIP

                if info.get_file_type() == Gio.FileType.DIRECTORY:
                    info.set_attribute_uint32("unix::mode", info.get_attribute_uint32("unix::mode") | stat.S_IWUSR)
                return OSTree.RepoCommitFilterResult.ALLOW

            modifier = OSTree.RepoCommitModifier.new(0, filter_func, None)

            checkout = self._get_system_checkout_path()
            destdir = checkout if os.path.exists(checkout) else None

            try:
                temp_dir = tempfile.mkdtemp(prefix=".", dir=destdir)
                # NOTE: tarfile has an issue with utf8. This works around the problem
                # by using the systems tar command.
                # Ref: https://bugzilla.redhat.com/show_bug.cgi?id=1194473
                subprocess.check_call(['tar', '-xf', tar, '-C', temp_dir])
                if self.user:
                    SystemContainers._correct_dir_permissions_for_user(temp_dir)

                repo.write_directory_to_mtree(Gio.File.new_for_path(temp_dir), mtree, modifier)
                root = repo.write_mtree(mtree)[1]

                metav = GLib.Variant("a{sv}", {'docker.layer': GLib.Variant('s', layer),
                                               'docker.size': get_directory_size(temp_dir)})
                csum = repo.write_commit(None, "", None, metav, root)[1]
            finally:
                shutil.rmtree(temp_dir, ignore_errors=True)

            repo.transaction_set_ref(None, "%s%s" % (OSTREE_OCIIMAGE_PREFIX, layer), csum)

        # create a $OSTREE_OCIIMAGE_PREFIX$image-$tag branch
        if not isinstance(manifest, str):
            manifest = json.dumps(manifest)

        metadata = GLib.Variant("a{sv}", {'docker.manifest': GLib.Variant('s', manifest)})
        mtree = OSTree.MutableTree()
        file_info = Gio.FileInfo()
        file_info.set_attribute_uint32("unix::uid", 0)
        file_info.set_attribute_uint32("unix::gid", 0)
        file_info.set_attribute_uint32("unix::mode", 0o755 | stat.S_IFDIR)

        dirmeta = OSTree.create_directory_metadata(file_info, None)
        csum_dirmeta = repo.write_metadata(OSTree.ObjectType.DIR_META, None, dirmeta)[1]
        mtree.set_metadata_checksum(OSTree.checksum_from_bytes(csum_dirmeta))

        root = repo.write_mtree(mtree)[1]
        csum = repo.write_commit(None, "", None, metadata, root)[1]
        repo.transaction_set_ref(None, imagebranch, csum)

        repo.commit_transaction(None)

    def _pull_docker_image(self, repo, image):
        with tempfile.NamedTemporaryFile(mode="w") as temptar:
            util.check_call(["docker", "save", "-o", temptar.name, image])
            return self._pull_docker_tar(repo, temptar.name, image)

    def _pull_docker_tar(self, repo, tarpath, image):
        temp_dir = tempfile.mkdtemp()
        try:
            with tarfile.open(tarpath, 'r') as t:
                t.extractall(temp_dir)
                manifest_file = os.path.join(temp_dir, "manifest.json")
                if os.path.exists(manifest_file):
                    manifest = ""
                    with open(manifest_file, 'r') as mfile:
                        manifest = mfile.read()
                    for m in json.loads(manifest):
                        if "Config" in m:
                            config_file = os.path.join(temp_dir, m["Config"])
                            with open(config_file, 'r') as config:
                                config = json.loads(config.read())
                                labels = config['config']['Labels']
                        imagename = m["RepoTags"][0] if m.get("RepoTags") else image
                        imagebranch = "%s%s" % (OSTREE_OCIIMAGE_PREFIX, SystemContainers._encode_to_ostree_ref(imagename))
                        input_layers = m["Layers"]
                        self._pull_dockertar_layers(repo, imagebranch, temp_dir, input_layers, labels=labels)
                else:
                    repositories = ""
                    repositories_file = os.path.join(temp_dir, "repositories")
                    with open(repositories_file, 'r') as rfile:
                        repositories = rfile.read()
                    imagename = list(json.loads(repositories).keys())[0]
                    imagebranch = "%s%s" % (OSTREE_OCIIMAGE_PREFIX, SystemContainers._encode_to_ostree_ref(imagename))
                    input_layers = []
                    for name in os.listdir(temp_dir):
                        if name == "repositories":
                            continue
                        input_layers.append(name + "/layer.tar")
                    self._pull_dockertar_layers(repo, imagebranch, temp_dir, input_layers)
            return imagename
        finally:
            shutil.rmtree(temp_dir)

    def _check_system_ostree_image(self, repo, img, upgrade):
        imagebranch = img.replace("ostree:", "", 1)
        current_rev = repo.resolve_rev(imagebranch, True)
        if not current_rev:
            raise ValueError("Internal error for image: %s.  Please pull the image again" % imagebranch)
        if not upgrade and current_rev[1]:
            return False
        remote, branch = imagebranch.split(":")
        return repo.pull(remote, [branch], 0, None)

    def _check_system_oci_image(self, repo, img, upgrade, src_creds=None):
        imagebranch = "%s%s" % (OSTREE_OCIIMAGE_PREFIX, SystemContainers._encode_to_ostree_ref(img))
        current_rev = repo.resolve_rev(imagebranch, True)
        if not current_rev:
            raise ValueError("Internal error for image: %s.  Please pull the image again" % imagebranch)
        if not upgrade and current_rev[1]:
            manifest = self._image_manifest(repo, current_rev[1])
            layers = SystemContainers.get_layers_from_manifest(json.loads(manifest))
            missing_layers = False
            for layer in layers:
                rev_layer = repo.resolve_rev("%s%s" % (OSTREE_OCIIMAGE_PREFIX, layer.replace("sha256:", "")), True)[1]
                if not rev_layer:
                    missing_layers = True
                    break
            if not missing_layers:
                return False

        try:
            d = util.Decompose(img)
            if d.registry == "":
                util.write_err("The image `{}` is not fully qualified.  The default registry configured for Skopeo will be used.".format(img))
        except:  # pylint: disable=bare-except
            # This is used only to raise a warning to the user, so catch any exception
            # and never let it fail.
            pass

        can_use_skopeo_copy = util.check_output([util.SKOPEO_PATH, "copy", "--help"]).decode().find("ostree") >= 0

        if can_use_skopeo_copy:
            return self._check_system_oci_image_skopeo_copy(repo, img, src_creds=src_creds)
        return self._check_system_oci_image_no_skopeo_copy(repo, img, imagebranch)

    def _check_system_oci_image_skopeo_copy(self, repo, img, src_creds=None):
        repo = self.get_ostree_repo_location()

        checkout = self._get_system_checkout_path()
        destdir = checkout if os.path.exists(checkout) else None
        temp_dir = tempfile.mkdtemp(prefix=".", dir=destdir)

        # Pass the original name to "skopeo copy" so we don't resolve it in atomic
        insecure, img = self._get_skopeo_args(img, full_resolution=False)

        destination = "ostree:{}@{}".format(img, repo)
        try:
            util.skopeo_copy("docker://" + img, destination, dest_ostree_tmp_dir=temp_dir, insecure=insecure, src_creds=src_creds)
        finally:
            shutil.rmtree(temp_dir, ignore_errors=True)
        return True

    def _check_system_oci_image_no_skopeo_copy(self, repo, img, imagebranch):
        try:
            manifest = self._skopeo_get_manifest(img)
        except ValueError:
            raise ValueError("Unable to find {}".format(img))
        layers = SystemContainers.get_layers_from_manifest(manifest)
        missing_layers = []
        for i in layers:
            layer = i.replace("sha256:", "")
            has_layer = repo.resolve_rev("%s%s" % (OSTREE_OCIIMAGE_PREFIX, layer), True)[1]
            if not has_layer:
                missing_layers.append(layer)
                util.write_out("Pulling layer %s" % layer)
        layers_dir = None
        try:
            layers_to_import = {}
            if len(missing_layers):
                layers_dir = self._skopeo_get_layers(img, missing_layers)
                for root, _, files in os.walk(layers_dir):
                    for f in files:
                        if f.endswith(".tar"):
                            layer_file = os.path.join(root, f)
                            layer = f.replace(".tar", "")
                            if not repo.resolve_rev("%s%s" % (OSTREE_OCIIMAGE_PREFIX, layer), True)[1]:
                                layers_to_import[layer] = layer_file
            self._import_layers_into_ostree(repo, imagebranch, manifest, layers_to_import)
        finally:
            if layers_dir:
                shutil.rmtree(layers_dir)
        return True

    @staticmethod
    def _generate_tmpfiles_data(missing_bind_paths):
        def _generate_line(x, state):
            return "%s    %s   0700 %i %i - -\n" % (state, x, os.getuid(), os.getgid())
        lines = []
        for x in missing_bind_paths:
            lines.append(_generate_line(x, "d"))
        return "".join(lines)

    @staticmethod
    def _get_commit_metadata(repo, rev, key):
        commit = repo.load_commit(rev)[1]
        metadata = commit.get_child_value(0)
        if key not in metadata.keys():
            return None
        return metadata[key]

    def extract(self, img, destination):
        """
        Extracts an image to a location on disk.

        :param img: Name of the image to extract.
        :type img: str
        :param destination: The location to extract the image.
        :type destination: str
        :returns: False if no ostree repo can be found
        :returns: None or False
        """
        repo = self._get_ostree_repo()
        if not repo:
            return False
        self._checkout(repo, img, img, 0, False, destination=destination, extract_only=True)

    @staticmethod
    def _encode_to_ostree_ref(name):
        def convert(x):
            return (x if str.isalnum(str(x)) or x in '.-' else "_%02X" % ord(x))

        if name.startswith("oci:"):
            name = name[len("oci:"):]
        registry, image, tag = SystemContainers._parse_imagename(name)
        if registry:
            fullname = "%s/%s:%s" % (registry, image, tag)
        else:
            fullname = "%s:%s" % (image, tag)

        ret = "".join([convert(i) for i in fullname])
        return ret

    @staticmethod
    def _decode_from_ostree_ref(name):
        try:
            l = []
            i = 0
            while i < len(name):
                if name[i] == '_':
                    l.append(str(chr(int(name[i+1:i+3], 16))))
                    i = i + 3
                else:
                    l.append(name[i])
                    i = i + 1
            return "".join(l)
        except ValueError:
            return name

    @staticmethod
    def _drop_sha256_prefix(img):
        if img.startswith("sha256:"):
            img = img.replace("sha256:", "", 1)
        return img

    @staticmethod
    def _get_ostree_image_branch(img):
        if "ostree:" in img:
            imagebranch = img.replace("ostree:", "")
        else: # assume "oci:" image
            img = SystemContainers._drop_sha256_prefix(img)
            imagebranch = "%s%s" % (OSTREE_OCIIMAGE_PREFIX, SystemContainers._encode_to_ostree_ref(img))
        return imagebranch

    def has_image(self, img):
        """
        Returns True if the image is local.

        :param img: The name of the img to look for.
        :type img: str
        :returns: True if the image is local, otherwise False
        :rtype: bool
        """
        repo = self._get_ostree_repo()
        if not repo:
            return False
        return bool(self._resolve_image(repo, img, allow_multiple=True))

    def _pull_dockertar_layers(self, repo, imagebranch, temp_dir, input_layers, labels=None):
        layers = {}
        next_layer = {}
        top_layer = None
        for i in input_layers:
            layer = i.replace("/layer.tar", "")
            layers[layer] = os.path.join(temp_dir, i)
            with open(os.path.join(temp_dir, layer, "json"), 'r') as f:
                json_layer = json.loads(f.read())
                parent = json_layer.get("parent")
                if not parent:
                    top_layer = layer
                next_layer[parent] = layer

        layers_map = {}
        enc = sys.getdefaultencoding()
        for k, v in layers.items():
            out = util.check_output([ATOMIC_LIBEXEC + '/dockertar-sha256-helper', v],
                                    stderr=DEVNULL)
            layers_map[k] = out.decode(enc).replace("\n", "")
        layers_ordered = []

        it = top_layer
        while it:
            layers_ordered.append(layers_map[it])
            it = next_layer.get(it)

        manifest = json.dumps({"Layers" : layers_ordered, "Labels" : labels})

        layers_to_import = {}
        for k, v in layers.items():
            layers_to_import[layers_map[k]] = v
        self._import_layers_into_ostree(repo, imagebranch, manifest, layers_to_import)

    def validate_layer(self, layer):
        """
        Validates a layer stored in ostree.

        :param layer: The layer to validate.
        :type layer: str
        :rtype: list
        """
        ret = []
        layer = layer.replace("sha256:", "")
        repo = self._get_ostree_repo()
        if not repo:
            return ret

        def validate_ostree_file(csum):
            _, inputfile, file_info, xattrs = repo.load_file(csum)
            # images are imported from layer tarballs, without any xattr.  Don't use xattr to compute
            # the OSTree object checksum.
            xattrs = GLib.Variant("a(ayay)", [])
            _, checksum_v = OSTree.checksum_file_from_input(file_info, xattrs, inputfile, OSTree.ObjectType.FILE)
            return OSTree.checksum_from_bytes(checksum_v)

        def traverse(it):
            def get_out_content_checksum(obj): return obj.out_content_checksum if hasattr(obj, 'out_content_checksum') else obj[1]
            def get_out_checksum(obj): return obj.out_checksum if hasattr(obj, 'out_checksum') else obj[1]
            while True:
                res = it.next()  # pylint: disable=next-method-called
                if res == OSTree.RepoCommitIterResult.DIR:
                    dir_checksum = get_out_content_checksum(it.get_dir())
                    dir_it = OSTree.RepoCommitTraverseIter()
                    dirtree = repo.load_variant(OSTree.ObjectType.DIR_TREE, dir_checksum)
                    dir_it.init_dirtree(repo, dirtree[1], OSTree.RepoCommitTraverseFlags.REPO_COMMIT_TRAVERSE_FLAG_NONE)
                    traverse(dir_it)
                elif res == OSTree.RepoCommitIterResult.FILE:
                    new_checksum = validate_ostree_file(get_out_checksum(it.get_file()))
                    if new_checksum != get_out_checksum(it.get_file()):
                        ret.append({"name" : it.get_file().out_name,
                                    "old-checksum" : it.get_file().out_checksum,
                                    "new-checksum" : new_checksum})
                elif res == OSTree.RepoCommitIterResult.ERROR:
                    raise ValueError("Internal error while validating the layer")
                elif res == OSTree.RepoCommitIterResult.END:
                    break

        current_rev = repo.resolve_rev("%s%s" % (OSTREE_OCIIMAGE_PREFIX, layer), False)[1]
        if not current_rev:
            raise ValueError("Layer not found: %s.  Please pull the image again" % layer.replace("sha256:", ""))

        it = OSTree.RepoCommitTraverseIter()
        it.init_commit(repo, repo.load_commit(current_rev)[1], OSTree.RepoCommitTraverseFlags.REPO_COMMIT_TRAVERSE_FLAG_NONE)
        traverse(it)
        return ret

    def tag_image(self, src, dest):
        """
        Tag an image.

        :param src: The source of the tag.
        :type src: str
        :param dest: The destination of the tag.
        :type src: str
        """
        def get_image_branch(img):
            img = SystemContainers._drop_sha256_prefix(img)
            return "%s%s" % (OSTREE_OCIIMAGE_PREFIX, SystemContainers._encode_to_ostree_ref(img))

        repo = self._get_ostree_repo()
        img_branch = get_image_branch(src)
        rev = repo.resolve_rev(img_branch, True)[1]
        if not rev:
            raise ValueError("Internal error for image: %s.  Please pull the image again" % img_branch)
        repo.prepare_transaction()
        repo.transaction_set_ref(None, get_image_branch(dest), rev)
        repo.commit_transaction(None)

    def get_storage_path(self, skip_canonicalize=False):
        """
        Returns the path to storage.

        :returns: The path to storage.
        :rtype: str
        """
        storage = os.path.sep.join([self._get_system_checkout_path(), ".storage"])
        if skip_canonicalize:
            return storage
        return self._canonicalize_location(storage)

    def _ensure_storage_for_image(self, repo, img):
        # Get the rev or raise out of the method
        try:
            _, rev = self._resolve_image(repo, img)[0]
        except (IndexError, TypeError):
            raise ValueError("Image {} not found".format(img))
        manifest = self._image_manifest(repo, rev)
        if manifest is None:
            raise ValueError("Image `%s` not present" % img)
        layers = SystemContainers.get_layers_from_manifest(json.loads(manifest))

        storage_path = self.get_storage_path()
        layers_dir = []
        for i in layers:
            layer = SystemContainers._drop_sha256_prefix(i)
            rootfs = os.path.sep.join([storage_path, layer])
            layers_dir.append(rootfs)
            if os.path.exists(rootfs):
                continue
            os.makedirs(rootfs)
            rootfs_fd = None
            try:
                rootfs_fd = os.open(rootfs, os.O_DIRECTORY)
                branch = "{}{}".format(OSTREE_OCIIMAGE_PREFIX, SystemContainers._drop_sha256_prefix(layer))
                rev_layer = repo.resolve_rev(branch, True)[1]
                if not rev_layer:
                    raise ValueError("Layer not found: %s.  Please pull the image again" % layer.replace("sha256:", ""))

                self._checkout_layer(repo, rootfs_fd, rootfs, rev_layer)
            finally:
                if rootfs_fd:
                    os.close(rootfs_fd)

        return layers_dir

    def _prune_storage(self, repo):
        storage = self.get_storage_path()
        if not os.path.exists(storage):
            return
        for i in os.listdir(storage):
            if i == "ostree":
                continue
            branch = "{}{}".format(OSTREE_OCIIMAGE_PREFIX, i)
            rev_layer = repo.resolve_rev(branch, True)[1]
            if not rev_layer:
                shutil.rmtree(os.path.join(storage, i))

    def mount_from_storage(self, img, destination, upperdir=None, workdir=None, debug=False):
        """
        Uses the system mount command to mount with overlay from storage.

        :param img: The name of the image to mount from.
        :type img: str
        :param destination: The location to mount to.
        :type desination: str
        :param upperdir: The higher level directory that takes precedence.
        :type upperdir: str
        :param workdir: Directory to prepare overlaying files
        :type workdir: str
        :param debug: If the command about to be executed should be printed.
        :type debug: bool
        :returns: Shell exit code from mount command
        :rtype: int
        """
        repo = self._get_ostree_repo()
        if not repo:
            raise ValueError("OSTree not supported")
        layers_dir = self._ensure_storage_for_image(repo, img)
        if upperdir is not None:
            cmd = "mount -t overlay overlay -olowerdir={},upperdir={},workdir={} {}".format(":".join(layers_dir), upperdir, workdir, destination)
        else:
            cmd = "mount -t overlay overlay -olowerdir={} {}".format(":".join(layers_dir), destination)
        if debug:
            util.write_out(cmd)
        stderr = None if debug else DEVNULL
        return util.check_call(cmd, stderr=stderr)

    @staticmethod
    def _correct_dir_permissions_for_user(path):
        os.chmod(path, 0o700)
        for root, dirs, files in os.walk(path, topdown=False, followlinks=True):
            if os.path.islink(root):
                continue
            for d in dirs:
                fullpath = os.path.join(root, d)
                if not os.path.islink(fullpath):
                    os.chmod(fullpath, 0o700)
            for f in files:
                fullpath = os.path.join(root, f)
                if not os.path.islink(fullpath):
                    s = os.stat(fullpath)
                    os.chmod(fullpath, s.st_mode | 0o600)

    def container_exec(self, name, detach, args):
        is_container_running = self._is_service_active(name)

        tty = os.isatty(0)

        if util.is_user_mode():
            raise ValueError("Command not supported in user mode")

        if is_container_running:
            cmd = [util.RUNC_PATH, "exec"]
            if tty:
                cmd.extend(["--tty"])
            if detach:
                cmd.extend(["--detach"])
            cmd.extend([name])
            cmd.extend(args)
            return util.check_call(cmd, stdin=sys.stdin, stderr=sys.stderr, stdout=sys.stdout)
        else:
            checkout = self._canonicalize_location(self.get_checkout(name))
            if checkout is None:
                raise ValueError("The container '{}' doesn't exist".format(name))
            if detach:
                raise ValueError("Cannot use --detach with not running containers")

            temp_dir = tempfile.mkdtemp()
            try:
                orig_config = os.path.sep.join([checkout, 'config.json'])
                with open(orig_config, 'r') as config_file:
                    config = json.loads(config_file.read())
                    new_path = os.path.realpath(os.path.normpath(os.path.join(checkout, config['root']['path'])))
                    config['root']['path'] = new_path
                    config['process']['args'] = args
                    config['process']['terminal'] = tty
                with open(os.path.sep.join([temp_dir, 'config.json']), 'w') as conf:
                    conf.write(json.dumps(config, indent=4))

                # The runtime directories are usually created by systemd when the
                # service starts.  Since we are running the container outside of
                # systemd, ensure that these directories are present on the host.
                for i in config['mounts']:
                    if 'type' not in i or i['type'] != 'bind' or 'source' not in i:
                        continue
                    src = i['source']
                    is_runtime = src.startswith('/run/') or src.startswith('/var/run/')
                    if is_runtime and not os.path.exists(src):
                        os.makedirs(src)

                cmd = [util.RUNC_PATH, "run"]
                cmd.extend([name])
                subp = subprocess.Popen(cmd,
                                        cwd=temp_dir,
                                        stdin=sys.stdin,
                                        stdout=sys.stdout,
                                        stderr=sys.stderr,
                                        close_fds=True,
                                        universal_newlines=False,
                                        env=os.environ)
                return subp.wait()
            finally:
                shutil.rmtree(temp_dir)
