#
# Copyright  (c) 2011-2016, Hortonworks Inc.  All rights reserved.
#
# Except as expressly permitted in a written agreement between your
# company and Hortonworks, Inc, any use, reproduction, modification,
# redistribution, sharing, lending or other exploitation of all or
# any part of the contents of this file is strictly prohibited.
#
#
# This script will install Hive ODBC Driver
# Run this script as root user

WORK_DIR=/tmp/hiveodbc
ODBC_DIR=/usr/lib/hive/lib/native

# function to check the version
function checkPythonVersion
{
 
  # expect this script to be in the location
  local versionScript="/tmp/pythonVersion.py"
  
  # check if python > 2.6 is installed
  local cmd="python2.6 $versionScript"
  echo $cmd
  eval $cmd
  if [ $? -eq 0 ]; then
    export GATEWAY_PYTHON_SCRIPT="python2.6"
    return
  fi

  # check if python > 2.6 is installed
  local cmd="python26 $versionScript"
  echo $cmd
  eval $cmd
  if [ $? -eq 0 ]; then
    export GATEWAY_PYTHON_SCRIPT="python26"
    return
  fi

  # check if python > 2.6 is installed
  local cmd="python $versionScript"
  echo $cmd
  eval $cmd
  if [ $? -eq 0 ]; then
    export GATEWAY_PYTHON_SCRIPT="python"
    return
  fi
  
  # check if python > 2.6 is installed
  local cmd="/opt/python/bin/python $versionScript"
  echo $cmd
  eval $cmd
  if [ $? -eq 0 ]; then
    export GATEWAY_PYTHON_SCRIPT="/opt/python/bin/python"
    return
  fi

  # check if python > 2.6 is installed
  local cmd="/opt/python2.7/bin/python2.7 $versionScript"
  echo $cmd
  eval $cmd
  if [ $? -eq 0 ]; then
    export GATEWAY_PYTHON_SCRIPT="/opt/python2.7/bin/python2.7"
    return
  fi

  # check if python > 2.6 is installed
  local cmd="/usr/bin/python $versionScript"
  echo $cmd
  eval $cmd
  if [ $? -eq 0 ]; then
    export GATEWAY_PYTHON_SCRIPT="/usr/bin/python"
    return
  fi

  # if you are here something went wrong
  echo "FATAL!!!!!!!!: COULD NOT FIND THE CORRECT PYTHON VERSION"
  exit
}

# check the python version
checkPythonVersion


if [ -f '/etc/SuSE-release' ]; then
  OS='suse11'
  INSTALL_PKG_CMD="zypper -n --no-gpg-checks install"
elif [ -f '/etc/redhat-release' ]; then
  rel_txt="`cat /etc/redhat-release`"
  INSTALL_PKG_CMD="yum -y install"
  cmd="echo '$rel_txt' | grep 'Red Hat.*6.[0-9]'"
  echo $cmd
  eval $cmd
  if [ $? -eq 0 ]; then
    OS='centos6'
  else
    cmd="echo '$rel_txt' | grep 'CentOS release.*6.[0-9]'"
    echo $cmd
    eval $cmd
    if [ $? -eq 0 ]; then
      OS='centos6'
    else
      OS='centos5'
    fi
  fi
else
  rel_txt="`cat /etc/issue`"
  cmd="echo '$rel_txt' | grep -q -i 'Debian.*6.0'"
  echo $cmd
  eval $cmd
  if [ $? -eq 0 ];then
    OS='debian'
  fi
  cmd="echo '$rel_txt' | grep -q -i 'Ubuntu 12.04'"
  echo $cmd
  eval $cmd
  if [ $? -eq 0 ]; then
    OS='debian'
  fi
  if [ 'xx' = "x${OS}x" ]; then
    echo "UN SUPPORTED PLATFORM: $rel_text"
    exit 1
  fi
  INSTALL_PKG_CMD="export DEBIAN_FRONTEND=noninteractive; apt-get -y --force-yes install"
fi

rel_txt="`uname -m`"
  cmd="echo '$rel_txt' | grep 'x86_64'"
  echo $cmd
  eval $cmd
  if [ $? -eq 0 ]; then
    bitness_str='x86_64'
    ODBC_SUBDIR='Linux-amd64-64'
  else
    bitness_str='i686'
    ODBC_SUBDIR='Linux-i386-32'
  fi
Driver_Version='2.0.0.1000'
rpm_name="hive-odbc-native-$Driver_Version-$OS.tar.gz"
ODBC_URL="http://public-repo-1.hortonworks.com/HDP/hive-odbc/2.0.0-1000/$OS/$rpm_name"
mkdir -p $WORK_DIR

#untar the file
echo "Untarring Hive odbc tar file"
cmd="cd ${WORK_DIR}; wget $ODBC_URL;tar -xzvf $rpm_name"
echo $cmd
eval $cmd

#Install dependencies
echo "Installing dependencies"
if [ $OS = 'debian' ]; then
  rpm_pkgs="libsasl2-modules-gssapi-mit"
else
  rpm_pkgs="cyrus-sasl-gssapi.x86_64 cyrus-sasl-plain.x86_64"
fi
cmd="$INSTALL_PKG_CMD $rpm_pkgs"
echo $cmd
eval $cmd


#Install Hive odbc rpm
echo "Installing Hive odbc rpm"
case $OS in

centos5) cmd="cd ${WORK_DIR}/hive-odbc-native-$Driver_Version-$OS;rpm -ivh hive-odbc-native-$Driver_Version-1.el5.$bitness_str.rpm"
         ;;
centos6) cmd="cd ${WORK_DIR}/hive-odbc-native-$Driver_Version-$OS;rpm -ivh hive-odbc-native-$Driver_Version-1.el6.$bitness_str.rpm"
         ;;
suse11) cmd="cd ${WORK_DIR}/hive-odbc-native-$Driver_Version-$OS;rpm -ivh hive-odbc-native-$Driver_Version-1.$bitness_str.rpm"
      ;;
debian) cmd="cd ${WORK_DIR}/hive-odbc-native-$Driver_Version-$OS;dpkg -i hive-odbc-native_$Driver_Version-2_amd64.deb"
      ;;

esac

echo $cmd
eval $cmd

#Install unixodbc
echo "Installing unixODBC"
if [ $OS = 'debian' ]; then
  cmd="wget ftp://ftp.unixodbc.org/pub/unixODBC/unixODBC-2.3.2.tar.gz;tar -zxvf unixODBC-2.3.2.tar.gz;cd unixODBC-2.3.2;./configure --enable-gui=no --enable-drivers=no --enable-iconv --with-iconv-char-enc=UTF8 --with-iconv-ucode-enc=UTF16LE --libdir=/usr/lib/x86_64-linux-gnu --prefix=/usr --sysconfdir=/etc;make install"
else
  cmd="$INSTALL_PKG_CMD unixODBC-devel.x86_64"
fi
echo $cmd
eval $cmd

#Set up Hive ODBC
echo "Copying .ini files to ${HOME}"
cmd="mv $ODBC_DIR/$ODBC_SUBDIR/hortonworks.hiveodbc.ini ${HOME}/;cp $ODBC_DIR/hiveodbc/Setup/odbc.ini ${HOME}/.odbc.ini;cp $ODBC_DIR/hiveodbc/Setup/odbcinst.ini $HOME/.odbcinst.ini"
echo $cmd
eval $cmd

#install pyodbc
cmd="cd ~/pyodbc-3.0.3;${GATEWAY_PYTHON_SCRIPT} setup.py build;${GATEWAY_PYTHON_SCRIPT} setup.py install"
echo $cmd
eval $cmd

echo "Update hortonworks.hiveodbc.ini"
file="${HOME}/hortonworks.hiveodbc.ini"
cmd="sed -i 's|^ODBCInstLib=libiodbcinst.so|#ODBCInstLib=libodbcinst.so|g' $file"
echo $cmd
eval $cmd
if [ $OS = 'suse11' ]; then
  cmd="sed -i 's|#ODBCInstLib=libodbcinst.a(libodbcinst.so.1)|ODBCInstLib=/usr/lib64/libodbcinst.so|g' $file"
elif [ $OS = 'debian' ]; then
  cmd="sed -i 's|#ODBCInstLib=libodbcinst.a(libodbcinst.so.1)|ODBCInstLib=/usr/lib/x86_64-linux-gnu/libodbcinst.so.2|g' $file"
else
  cmd="sed -i 's|#ODBCInstLib=libodbcinst.a(libodbcinst.so.1)|ODBCInstLib=/usr/lib64/libodbcinst.so.2|g' $file"
fi

echo $cmd
eval $cmd

cmd="cp ${HOME}/hortonworks.hiveodbc.ini /home/$1/.hortonworks.hiveodbc.ini"
echo $cmd
eval $cmd

cmd="chmod 777 /home/$1/.hortonworks.hiveodbc.ini"
echo $cmd
eval $cmd

cmd="chown hrt_qa /home/$1/.hortonworks.hiveodbc.ini"
echo $cmd
eval $cmd
