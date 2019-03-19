import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(name='jobs',
      version='0.1',
      description='Python interfaces for creating and submitting LSF and SLURM jobs',
      long_description=long_description,
      long_description_content_type="text/markdown",
      url='http://github.com/bchalios/jobs.git',
      author='Babis Chalios',
      author_email='babis.chalios@bsc.es',
      license='GPL3',
      packages=['jobs'],
      zip_safe=False)
