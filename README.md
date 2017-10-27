### Prerequisites

`TODO`

1. Install MongoDB, unzip it

2. Create a directory that you'd want mongodb to store all its data in.

3. In a separate shell, run the command, `mongod --dbpath=<dir in 2.>`. Note that the executable is located in the MongoDB that you have installed, and unzipped.

4. Assuming that the virtualenv is set up, activate it. Then run `python import.py`

<hr/>

### <a name="set_virtualenv"></a> Setting up virtualenv

The following code will circumvent the restriction where users are not able to use `pip` to install the package in `/usr/lib/python2.7/site-packages`

1. Download the `virtualenv` source code, and run the script locally. At the end, you should get the full path of the file, `virtualenv.py`

```
$ curl -O https://pypi.python.org/packages/source/v/virtualenv/virtualenv-1.9.tar.gz
$ tar xvfz virtualenv-1.9.tar.gz
$ cd virtualenv-1.9
$ realpath virtualenv.py
$ # The full path of virtualenv.py will be obtained, take note of the path
$ # A possible output: /home/stuproj/cs4224f/virtualenv-1.9/virtualenv.py (results will vary)
```

2. Ensure that the `virtualenv.py` is executable

```
$ chmod +x virtualenv.py
```

<hr/>

### <a name="get_mognoimport"></a> Obtaining File Path of Mongoimport

1. Run the following script to get all the possible path of cqlsh
```
$ whereis mongoimport
$ # Possible output
$ # mongoimport: /usr/bin/mongoimport /temp/cs4224f/mongodb/bin/mongoimport
```

2. Ensure that the full path of the executable permits you to run query on it. Make sure the `mongoimport` is the one you intend to use.

<hr/>

### Setup

0. Download and unzip the project repository - we assume that the project repository zip file downloaded is `cs4224f-mongodb-master.zip`, however, this might change in the future.

```
$ # Assume that the project directory is placed in the home directory
$ cd ~
$ # Assume the project folder is called cs4224f-mongodb-master.zip
$ # However, this assumption may not hold in future release - hence make changes accordingly
$ unzip cs4224f-mongodb-master.zip
```

1. Create an environment directory, `env`. We assume that you have set up virtualenv using the above - hence replace the variable \<virtualenv\> with the full path of the file, `virtualenv.py`. Otherwise, you may simply replace \<virtualenv\> with `virtualenv`.

```
$ # Ensure that you are at the root of the project repository
$ cd ~/cs4224f-mongodb-master
$ <virtualenv> env
$ # The following is an example
$ # /home/stuproj/cs4224f/virtualenv-1.9/virtualenv.py env
```

2. Install all dependencies in requirement.txt

```
$ source ./env/bin/activate
(env) $ # You should see (env) prepended to the command prompt
(env) $ pip install -r requirements.txt
```

3. Download the dataset if you have not, and move them to `/data` folder. Thereafter, move all transaction files to the `/xact/` folder.

```
(env) $ wget http://www.comp.nus.edu.sg/~cs4224/4224-project-files.zip
(env) $ unzip 4224-project-files.zip
(env) $ mv ./4224-project-files/data-files/*.csv ./data
(env) $ mv ./4224-project-files/xact-files/* ./xact/
```

4. Set up the configuration file. `config.conf`.

5. Run `import.py` to import the data into cassandra

6. You will be prompted to confirm the correct `mongoimport` path
