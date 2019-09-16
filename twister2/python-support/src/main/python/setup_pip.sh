# Creating distribution
python3 setup.py bdist_wheel

# Installing Locally
python3 -m pip install dist/twister2-0.1.1-py3-none-any.whl

# Uploading to PIP
python3 -m twine upload dist/*
