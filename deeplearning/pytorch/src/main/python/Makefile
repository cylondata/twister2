clean-dist:
	rm -rf dist

clean-build:
	rm -rf build

clean:
	make clean-dist;make clean-build

build:
	python3 setup.py sdist bdist_wheel

upload:
	python3 -m twine upload --repository-url https://test.pypi.org/legacy/ dist/*

release:
	make clean; make build; make upload

install:
	python3 -m pip install --index-url https://test.pypi.org/simple/ --no-deps twister2-deepnet-test