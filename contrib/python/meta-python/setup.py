try:
    from setuptools import setup
    # hush pyflakes
    setup
except ImportError:
    from distutils.core import setup

requires = ['zkpython>=0.4']

packages=['metaq']

setup(
    name = 'metaq',
    version = '0.2',
    description= 'Metaq client for python.',
    author = 'killme2008',
    author_email = 'killme2008@gmail.com',
    license = 'Apache License 2.0',
    url = 'https://github.com/killme2008/Metamorphosis/tree/master/contrib/python/meta-python',
    install_requires = requires,
    packages = packages,
    classifiers = [
        "Development Status :: 4 - Beta",
        "Environment :: Console",
        "Intended Audience :: Developers",
        "Intended Audience :: System Administrators",
        'License :: OSI Approved :: Apache Software License',
        "Programming Language :: Python",
        "Programming Language :: Python :: 2.6",
        "Programming Language :: Python :: 2.7",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.1",
        "Programming Language :: Python :: 3.2",
        "Topic :: Software Development :: Build Tools",
        "Topic :: Software Development :: Libraries :: Python Modules",
        ],
    )
