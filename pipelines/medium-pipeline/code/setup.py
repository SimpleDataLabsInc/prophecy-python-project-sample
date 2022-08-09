from setuptools import setup, find_packages
setup(
    name = 'medium-pipeline',
    version = '1.0',
    packages = find_packages(include = ('mediumpipeline*', )),
    description = 'workflow',
    install_requires = [
'prophecy-libs==1.3.1'],
    entry_points = {
'console_scripts' : [
'main = mediumpipeline.pipeline:main', ], },
    extras_require = {
'test' : ['pytest', 'pytest-html'], }
)
