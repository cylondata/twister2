#  // Licensed under the Apache License, Version 2.0 (the "License");
#  // you may not use this file except in compliance with the License.
#  // You may obtain a copy of the License at
#  //
#  // http://www.apache.org/licenses/LICENSE-2.0
#  //
#  // Unless required by applicable law or agreed to in writing, software
#  // distributed under the License is distributed on an "AS IS" BASIS,
#  // WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  // See the License for the specific language governing permissions and
#  // limitations under the License.

import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="twister2-deepnet-test",  # Replace with your own username
    version="0.0.8",
    author="Twister2 Developers",
    author_email="twister2@googlegroups.com",
    description="Twister2 DeepNet",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/DSC-SPIDAL/twister2/tree/master/deeplearning/",
    packages=setuptools.find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires='>=3.7',
    install_requires=[
        'jep==3.9.0',
        'cloudpickle',
        'numpy',
        'py4j',
        'mpi4py',
    ],
)
