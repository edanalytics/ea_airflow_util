import pathlib
import setuptools

HERE = pathlib.Path(__file__).parent
README = (HERE / "README.md").read_text()

setuptools.setup(
      name='ea_airflow_util',
      version='0.5.0',
      description='EA Airflow tools',
      license_files=['LICENSE'],
      url='https://github.com/edanalytics/ea_airflow_util',

      author='Jay Kaiser',
      author_email='jkaiser@edanalytics.org',

      long_description=README,
      long_description_content_type='text/markdown',

      packages=setuptools.find_packages(),
      install_requires=[
          'apache-airflow-client',
          'requests',
      ],
      zip_safe=False
)
