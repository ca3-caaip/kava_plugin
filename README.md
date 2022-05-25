# kava plugin

## docker

### For start

$ docker-compose up -d

### For access to shell in the container

$ docker-compose exec kava_plugin bash

### For end

$ docker-compose down

### For remove

$ docker-compose down --rmi all --volumes --remove-orphans

### For test

in the container

```
$ cd /app
$ curl -sSL https://install.python-poetry.org | python -
$ echo export PATH="$HOME/.local/bin:$PATH" >> ~/.bashrc
$ source ~/.bashrc
$ poetry config virtualenvs.in-project true && poetry install
$ poetry shell
$ pytest --cov=src --cov-branch --cov-report=term-missing -vv
```

### For execution

in the container

```
$ cd /app
$ curl -sSL https://install.python-poetry.org | python -
$ export PATH="$HOME/.local/bin:$PATH"
$ poetry config virtualenvs.in-project true && poetry install
$ poetry shell
$ python src/main.py address > result.csv
e.x. $ python src/main.py kava1af7lm2qv9zp526gjd3cdxrpr9zeangjlyhjqjx > result.csv
```
