README
======

#### Installation:

If using an Ubuntu 12/13 enviornment, several packages are required (or beneficial) to be installed before pulling from the directory.

```
sudo apt-get install -y git
sudo apt-get install -y redis-server
sudo apt-get install -y python-pip
sudo apt-get install -y python-zmq
sudo apt-get install -y p7zip-full
sudo apt-get install -y python-mysqldb
sudo apt-get install -y python-Levenshtein
```

#### Installing the repository

```
git clone git@github.com:funginstitute/patentprocessor
```

After cloning, install the packages via PIP

```
cd patentprocessor
sudo pip install -r requirements.txt
```

Download:

  * [Location Table](https://s3.amazonaws.com/funginstitute/geolocation_data.sqlite3). Place this file in the `lib` directory

#### Collaborating to the repository

Rather than cloning the repository, fork it and issue pull requests. To keep your personal repository up to date, we set up `.git/config` to include upstream as follows:

```
...

[remote "upstream"]
        url = https://github.com/funginstitute/patentprocessor.git
        fetch = +refs/heads/*:refs/remotes/upstream/*
[remote "origin"]
        fetch = +refs/heads/*:refs/remotes/origin/*
        url = git@github.com:[your_username]/patentprocessor.git

...
```

Once that is complete, we can fetch and merge.

```
git fetch upstream
git merge upstream\[branch]
```

Issue pull requests to the [FungInstitute GitHub](https://github.com/funginstitute/patentprocessor) repository and the orginators will take a look at the code being modified.

#### Some MySQL recipes specific to AWS:

Export files into CSV

```
mysql -u [user] -p [passwd] --database=[db] --host=[host] --batch -e "select * from [table] limit 10" | sed 's/\t/","/g;s/^/"/;s/$/"/;s/\n//g' > [table].csv
```

Allow local file reading (local-infile must be 1 for security purposes)

```
mysql -u [user] -p --local-infile=1 -h [db] [tbl]
```

#### Other notes

  * [Adding Indices to SQLAlchemy](http://stackoverflow.com/questions/6626810/multiple-columns-index-when-using-the-declarative-orm-extension-of-sqlalchemy)
  * [Ignoring Files in GIT](https://help.github.com/articles/ignoring-files)
  * [Permanently removing files in GIT](http://dalibornasevic.com/posts/2-permanently-remove-files-and-folders-from-a-git-repository)