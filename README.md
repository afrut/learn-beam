- See here for setting up the environment: https://beam.apache.org/get-started/quickstart-py/#set-up-your-environment
- Use bash as the shell.
- Activate the environment.
```
source env/bin/activate
```
- To start a workflow:
```
python some_file.py
```

- Install dependencies
```
pip install apache-beam
pip install apache-beam[gcp,kafka]
```