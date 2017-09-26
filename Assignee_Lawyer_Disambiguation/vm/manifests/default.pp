# run apt-get update 
exec { 'apt-get update':
  command => '/usr/bin/apt-get update',
}

Package { ensure => present }

$aptpackages = ['python-dev','p7zip','git','vim','ipython','make','libmysqlclient-dev','python-mysqldb','python-pip','gfortran','libopenblas-dev','liblapack-dev','g++','sqlite3','libsqlite3-dev','python-sqlite','mysql-server-5.5']
#$pippackages = ['flask','requests','supervisor','netifaces','redis','celery']

package { $aptpackages:
    require => [ Exec['apt-get update'] ],
  }

#package { $pippackages:
#    require => [ Package[$aptpackages] ],
#    provider => pip,
#}
