- See here for setting up the environment: https://beam.apache.org/get-started/quickstart-py/#set-up-your-environment
- Use ubuntu/bash as the shell.
- Install docker: https://docs.docker.com/engine/install/ubuntu/
  - Start the docker daemon
    ```
    sudo docker run hello-world
    ```
  - Test docker installation
    ```
    sudo docker run hello-world
    ```
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