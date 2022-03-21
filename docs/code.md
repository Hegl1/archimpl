# CODE

For our project we use Python3 and manage dependencies using [pipenv](https://pipenv.pypa.io/en/latest/#install-pipenv-today).
To install a working environment run `pipenv install ; pipenv install --dev`.
With the installed python module `python3 -m pipenv install ; python3 -m pipenv install --dev` can be used.

## Execution
For execution of the programm you need to use the created enviroment by running `pipenv shell` or with the python module `python3 -m pipenv shell`. 
To start the programm you then can use the [main.py file](/project/src/mosaic/main.py) by calling `python3 main.py` from the `/project/src/mosaic/` directory.

## Testing
All the test files can be found at `/project/tests/`.
For testing you can use the [Makefile](/project/Makefile) in the project folder.
To execute all test just run `make test all`.

## Used packages
Used packeages can be seen in the [Pipfile](../Pipfile).