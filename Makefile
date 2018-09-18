test:
	python -m unittest discover -v

coverage:
	coverage run -m unittest discover -v

lint:
	pylint --disable=all --enable=F,E,unreachable,duplicate-key,unnecessary-semicolon,global-variable-not-assigned,unused-variable,binary-op-exception,bad-format-string,anomalous-backslash-in-string,bad-open-mode azfilebak
