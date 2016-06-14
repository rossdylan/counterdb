from setuptools import setup, find_packages
setup(
    name='counterdb',
    author='Ross Delinger',
    author_email='rossdylan@fastmail.com',
    version='0.1.0',
    install_requires=['uvloop', 'python-etcd', 'lmdb', 'msgpack-python', 'cchardet', 'aiohttp'],
    packages=find_packages(),
    entry_points="""
    [console_scripts]
    counterdb=counterdb:main
    """);
