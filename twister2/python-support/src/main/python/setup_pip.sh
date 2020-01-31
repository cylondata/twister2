# Cleanup
rm build -R
rm dist -R
rm twister2.egg-info -R

# set DEV true for development testing
# set DEV false for production release
DEV=true
# set release version of the API
VERSION=0.1.27

# Creating distribution
python3 setup.py bdist_wheel

if [ ${DEV} = true ]
then
  echo "=================Development====================="
  # Installing Locally
  #python3 -m pip install dist/twister2-0.1.21-py3-none-any.whl #--user
  pip3 install dist/twister2-${VERSION}-py3-none-any.whl #--user
else
  echo "=================Production================"
  # Installing Locally
  python3 -m pip install dist/twister2-${VERSION}-py3-none-any.whl #--user
  #pip3 install dist/twister2-0.1.27-py3-none-any.whl --user
  # Uploading to PIP
  python3 -m twine upload dist/*
fi









