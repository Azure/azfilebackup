test:
	python -m unittest discover -v

coverage:
	coverage run -m unittest discover -v

lint:
	pylint --disable=all --enable=F,E,unreachable,duplicate-key,unnecessary-semicolon,global-variable-not-assigned,unused-variable,binary-op-exception,bad-format-string,anomalous-backslash-in-string,bad-open-mode azfilebak

docker-build:
	docker build -t azfilebak .

# File test.env can be used to pass a storage account key, e.g.
# STORAGE_KEY='xxx'
# Don't check in that file.

docker-test:
	docker run -t --env-file test.env azfilebak

docker-clean:
	docker rm -f `docker ps -qa`
