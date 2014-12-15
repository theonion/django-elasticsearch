from distutils.core import setup


setup(
    name='django-elasticsearch',
    version='0.0.1',
    packages=[
        'djelastic',
    ],
    install_requires=[
        'Django==1.7.1',
        'elasticsearch==1.2.0',
        'elasticsearch-dsl==0.0.2',
        'six==1.8.0',
    ]
)
