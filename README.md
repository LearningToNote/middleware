# middleware

This is a server component that handles some business logic and mediates between frontend and database interfaces.

## Requirements

1. Python 2.7.10 is required.
2. To install Python dependencies use `pip install -r requirements.txt`. Essentially we need `flask` plus two plugins `flask-cors` and `flask-login`, `metapub` for PubMed import, and a fork of `pyhdb`, which is a yet-unmerged pull-request to the official pyhdb repository.
3. The server will assume that the [database schema](https://github.com/LearningToNote/importers/tree/master/db_setup) is set up properly.
4. A valid `secrets.json` file is required in the root folder of the script. It should contain the address, port, and credentials information used to connect to the database (SAP HANA). A sample is given in `secrets.json.example`.
5. For https, the server will look for a certificate (`certificate.crt`) and a key (`certificate.key`) file in its root directory.

## Running the Server

Simply run `python server.py [staticdir='static/']`. 
The optional parameter `staticdir` is a path to a directory from which files will be available statically.

(See [this repository](https://github.com/LearningToNote/frontend) for the embedded use case)
