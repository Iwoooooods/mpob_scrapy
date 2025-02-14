pip install -r requirements.txt --no-build-isolation
FOR /F %%i IN ('python setup.py -V') DO set VERSION=%%i
scrapyd-deploy -v %VERSION%