from setuptools import setup

with open("README.md", "r") as fh:
    long_description = fh.read()

setup(
    name='freewheel4py',
    version='0.0.1',
    description='make working with the freewheel publisher api easy.',
    py_modules=["nfreewheel4py"],
    package_dir={'': 'src'},
    extras_require={
        "dev": [
            "pytest >= 3.9",
            "check-manifest",
            "twine"
        ]
    },
    classifiers=[
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Topic :: Software Development :: Libraries :: Python Modules",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent"
    ],
    python_requires='>=3',
    long_description=long_description,
    long_description_content_type="text/markdown",
    author="Dylan Marsella",
    author_email="dylanmarsella1@gmail.com",
    url="https://github.com/dmars12345/freewheel4py"
)
