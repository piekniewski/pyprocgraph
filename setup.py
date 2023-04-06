from setuptools import setup

setup(name='pyprocgraph',
      version='0.0.1',
      python_requires='>=3',
      description='Python Compute Graph - Wrapper for multiprocessing module to ' +
                  'build a web of processing workers connected via queues.',
      packages=['pyprocgraph'],
      install_requires=[
          'psutil',
      ],
      zip_safe=False,
     )
