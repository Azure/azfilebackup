# CHANGELOG

## v1.0-beta1

This release contains some fixes as well as the changes below.

### Change to backup blob name format

The backup blobs (archives) are now saved using the following name format:

```
fs_test-backup_full_20181121_164327.tar.gz
```

Which is, `*fileset*_*vmname*_*timestamp*.tar.gz`. The default fileset name is `fs`.

### Display archive size and readable date in listing

The `--list` command will now display the full blob name, its size in bytes and its creation date in human readable format.

```
# azfilebak --list
2018-11-21 16:46:24+00:00    512754746 fs_test-backup_full_20181121_164327.tar.gz
2018-11-22 09:42:00+00:00    512782875 fs_test-backup_full_20181122_094011.tar.gz
```

### Restore archive using blob name

The `--restore` command now accepts the full blob name (also works with the `--stream` option):

```
# azfilebak --restore fs_test-backup_full_20181122_094011.tar.gz
```

### Override container name

All backup files are saved in a container bearing the same name as the virtual machine the tool runs on. When listing or restoring archive files, it is now possible to override this container name, so that archives from another machine can be listed and restored.

```
# azfilebak --list --container hec99v106014
# azfilebak --restore tmpdir_hec99v106014_full_20181122_110732.tar.gz --container hec99v106014
```

## v1.0-alpha2

### Release process

This new release has proper naming and versioning for the RPM package. This means that to install this release you will have to remove the previous package (with wrong naming) and install the new one. Further releases should be installable using the rpm/zypper upgrade commands.

Remove old package:

```
sudo zypper rm virtualenv-.
```

Install new package:

```
sudo zypper in ./azfilebackup-1.0-0.pre.a2.x86_64.rpm
```

The executable is installed in this location: `/usr/share/python/bin/azfilebak`.

### Use default location for pidfile

The PID file is now created in the default system location (`/var/run`). This means that the script always has to be run as root in order to be able to create this file. The PID file is used to ensure that two full backups can not be run at the same time. If you try to launch a second full backup, you will see the following message:

```
2018-10-16 09:38:32,299 pid-20712 line-173 WARNING: "Skip full backup, already running"
```

### Updated logging configuration

The default logging level has been changed to INFO in order to limit the amount of messages displayed. A new option (`-d`, `--debug`) allows you to put the script back into DEBUG mode if necessary. Some additional messages are now displayed at the INFO level in order to confirm that the blob was saved to storage. Finally, the messages are now sent to STDERR instead of a file, so that the log can be redirected from the crontab, i.e. using the following stanza to redirect standard outputs:

```
>/var/log/azfilebackup.log 2>&1
```

Here is an example of the messages displayed on STDERR for a full backup at the default INFO level:

```
2018-10-16 09:38:26,766 pid-20702 line-159 INFO: "Backup request for default fileset: ase"
2018-10-16 09:38:26,768 pid-20702 line-180 INFO: "Backup request for fileset: ase"
2018-10-16 09:38:28,180 pid-20702 line-51 INFO: "Executing tar cpzf - --hard-dereference --exclude /install --exclude /sybase/AZ3/saparch_1 --exclude /sybase/AZ3/sapdata_1 --exclude /sybase/AZ3/saplog_1 --exclude /sybase/AZ3/saptemp_1 --exclude /dev --exclude /run --exclude /sys --exclude /proc /"
2018-10-16 09:38:28,182 pid-20702 line-208 INFO: "Streaming backup to blob: ase_full_20181016_093826.tar.gz in container: test-backup"
2018-10-16 09:40:58,452 pid-20702 line-223 INFO: "Finished streaming blob: ase_full_20181016_093826.tar.gz"
```

### Send notification upon backup completion

Upon backup completion (successful or failed), a JSON message will be sent using a command specified in the configuration file. Below is an example of the message contents.

```json
{
	"cloud": "azure",
	"hostname": "hec99v106014",
	"instance-id": "AFD83530-840D-11E8-9E6C-FC820C452436",
	"state": "success",
	"type": "fs",
	"method": "file",
	"level": "full",
	"account-id": "2e394ee6-2714-4080-88c3-ecfc33d85147",
	"customer-id": "AZ1",
	"system-id": "AZ3",
	"database-name": "",
	"database-id": "",
	"s3-path": "sahec99az1backup0001.blob.core.windows.net/hec99v106014/tmp_dir_full_20181022_171453.tar.gz",
	"timestamp-send": 1540221294,
	"timestamp-last-successful": 1540221293,
	"timestamp-bkp-begin": "",
	"timestamp-bkp-end": 1540221293,
	"backup-size": 120,
	"dbtype": "",
	"error-message": "",
	"script-version": "1.0-alpha2"
}
```
