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
