import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name='Twister2',
    version='0.1',
    author="Twister2 Developers",
    author_email="twister2@googlegroups.com",
    description="Twister2 is a composable big data environment supporting streaming, data pipelines and analytics. Our vision is to build robust, simple to use data analytics solutions that can leverage both clouds and high performance computing infrastructure.",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/DSC-SPIDAL/twister2",
    packages=setuptools.find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: Apache 2.0 License",
        "Operating System :: OS Independent",
    ],
    python_requires='>=3.6',
)
