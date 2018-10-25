# coding=utf-8

"""
Utility module to read and parse a GNU tar snapshot file.
"""

import time

class DirectoryRecord(object):
    """A directory record contains a set of metadata describing a particular directory."""
    def __init__(self, nfs, timestamp_sec, timestamp_nsec, dev, ino, name):
        self.nfs = nfs
        self.timestamp_sec = timestamp_sec
        self.timestamp_nsec = timestamp_nsec
        self.dev = dev
        self.ino = ino
        self.name = name
        self.files = []

    def add_file(self, control_code, filename):
        """Add a file to the directory."""
        self.files.append((control_code, filename))

    def find_files(self, pattern):
        """Return a list of files matching the pattern."""
        return [self.name + '/' + f[1] for f in self.files if pattern in f[1]]

class Snapshot(object):
    """Read and parse a GNU tar snapshot file."""

    def __init__(self, snap_file):
        self.directory_records = []
        self.read_snapshot(snap_file)

    def read_until_nul(self, file_desc):
        """Read bytes until a NUL value is encountered."""
        read_bytes = ''
        while True:
            byte = file_desc.read(1)
            if not byte or byte == '\x00':
                return read_bytes
            read_bytes += byte

    def read_snapshot(self, snap_file):
        """Read a snapshot file."""
        with open(snap_file, "rb") as snap_fd:
            self.header = snap_fd.readline().strip()
            self.last_backup_sec = int(self.read_until_nul(snap_fd))
            self.last_backup_nsec = int(self.read_until_nul(snap_fd))

            # Read directory records
            while True:
                nfs = self.read_until_nul(snap_fd)

                # If EOF, no more records
                if not nfs:
                    break

                nfs = int(nfs)
                timestamp_sec = int(self.read_until_nul(snap_fd))
                timestamp_nsec = int(self.read_until_nul(snap_fd))
                dev = int(self.read_until_nul(snap_fd))
                ino = int(self.read_until_nul(snap_fd))
                name = self.read_until_nul(snap_fd)

                record = DirectoryRecord(
                    nfs,
                    timestamp_sec,
                    timestamp_nsec,
                    dev,
                    ino,
                    name
                )

                self.directory_records.append(record)

                while True:
                    # Read dumpfile contents
                    control_code = snap_fd.read(1)
                    if not control_code or control_code == '\x00':
                        break
                    filename = self.read_until_nul(snap_fd)
                    record.add_file(control_code, filename)

                # NUL before next directory record
                nul = snap_fd.read(1)

    def find_files(self, pattern):
        """Return a list of files matching the pattern across all directories."""
        matching_files = []
        for d in self.directory_records:
            files = d.find_files(pattern)
            if files:
                matching_files.extend([(d.timestamp_sec, f) for f in files])
        return matching_files

    def as_string(self):
        """Return the snapshot contents as a readable document."""
        output = ''
        output += 'Last backup: ' + time.asctime(time.localtime(self.last_backup_sec)) + '\n\n'
        for d in self.directory_records:
            for f in d.files:
                t = time.asctime(time.localtime(d.timestamp_sec))
                output += t + ' ' + d.name + '/' + f[1] + '\n'
        return output

    def as_csv(self):
        """Return the snapshot contents as a CSV document."""
        output = ''
        for d in self.directory_records:
            for f in d.files:
                t = time.strftime("%Y-%m-%dT%H:%M:%S", time.localtime(d.timestamp_sec))
                output += t + ',' + d.name + '/' + f[1] + '\n'
        return output
