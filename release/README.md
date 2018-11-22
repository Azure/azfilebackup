# Releasing an RPM package

Use the provided Dockerfile to launch the release environment:

```
docker build -t release-leap .
docker run -it --rm -v `pwd`:/data release-leap
```

Once in the container, clone the repo with the release tag:

```
git clone https://github.com/Azure/azfilebackup -b v1.0-alpha1
```

Package the RPM using FPM, make sure to convert the version number to RPM format:

```
fpm.ruby2.5 -s virtualenv -t rpm -n azfilebackup --version 1.0 --iteration 0.pre.a1 --rpm-auto-add-directories .
```

Copy the generated RPM out of the container:

```
cp azfilebackup-1.0-0.pre.a1.x86_64.rpm /data/
```
