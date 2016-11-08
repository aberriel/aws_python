_PHONY: documentation

documentation:
	rm -rf doc/*
	mkdir -p doc
	pydoc -w ./
	mv *.html doc
