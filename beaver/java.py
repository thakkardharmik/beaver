import logging
import os

from beaver.config import Config
from beaver.machine import Machine

logger = logging.getLogger(__name__)


class Java(object):
    _JAVA_HOME = Config.get('machine', 'JAVA_HOME')
    _JAVA_CMD = os.path.join(_JAVA_HOME, 'bin', 'java')
    _JAVAC_CMD = os.path.join(_JAVA_HOME, 'bin', 'javac')
    _KEYTOOL_CMD = os.path.join(_JAVA_HOME, 'bin', 'keytool')
    _BEAVER_JAVA_BASE_DIR = os.path.join(Config.getEnv('WORKSPACE'), 'beaver', 'java')
    _BEAVER_JAVA_COMMON_SRC_DIR = os.path.join(_BEAVER_JAVA_BASE_DIR, 'common', 'src')
    _BEAVER_JAVA_TEZ_EXAMPLES_DIR = os.path.join(_BEAVER_JAVA_BASE_DIR, 'tez')
    _QE_UTILS_SRC_DIR = os.path.join(_BEAVER_JAVA_COMMON_SRC_DIR, 'com', 'hortonworks', 'qe', 'utils')

    def __init__(self):
        pass

    @classmethod
    def getBeaverJavaCommonSrcDir(cls):
        return cls._BEAVER_JAVA_COMMON_SRC_DIR

    @classmethod
    def getBeaverJavaTezExamplesDir(cls):
        '''
        Returns WORKSPACE/beaver/java/tez
        '''
        return cls._BEAVER_JAVA_TEZ_EXAMPLES_DIR

    @classmethod
    def getQEUtilsSrcDir(cls):
        return cls._QE_UTILS_SRC_DIR

    @classmethod
    def runJavac(cls, sourceFiles, targetDir, classPath=None):
        '''
        Runs javac to compile source files with current user. Class files will be in targetDir.
        Returns (exit_code, stdout)
        '''
        if classPath is None:
            classPathStr = ""
        else:
            classPathStr = " -classpath " + classPath
        Machine.makedirs(user=None, host=None, filepath=targetDir, passwd=None)
        (exit_code, output) = Machine.run(cls._JAVAC_CMD + classPathStr + " -d %s %s" % (targetDir, sourceFiles))
        return (exit_code, output)

    @classmethod
    def runJava(cls, workingDir, fqClassName, classPath=None, cmdArgs=None):
        if not cmdArgs:
            cmdArgs = []

        if classPath is None:
            classPathStr = ""
        else:
            classPathStr = " -classpath " + classPath
        cmdArgsStr = ''
        for cmdArg in cmdArgs:
            cmdArgsStr += ' %s' % cmdArg
        (exit_code, output) = Machine.run(
            cls._JAVA_CMD + classPathStr + " %s %s" % (fqClassName, cmdArgsStr), cwd=workingDir
        )
        return (exit_code, output)

    @classmethod
    def runKeyTool(cls, command, options=None):
        if not options:
            options = {}

        cmd = " -%s" % command
        for key, value in options.items():
            cmd += " %s %s" % (key, value)
        (exit_code, output) = Machine.run(cls._KEYTOOL_CMD + cmd)
        return (exit_code, output)

    @classmethod
    def runOpenSslTool(cls, command, options=None):
        if not options:
            options = {}

        cmd = " %s" % command
        for key, value in options.items():
            cmd += " %s %s" % (key, value)
        (exit_code, output) = Machine.run('openssl' + cmd)
        return (exit_code, output)

    @classmethod
    def createKeyStores(cls, storedir, hosts=None, certify_key=False):
        if not hosts:
            hosts = ['localhost']
        if certify_key:
            certFile = os.path.join(storedir, 'ca-cert')
            keyFile = os.path.join(storedir, 'ca-key')
            exit_code, _output = cls.runOpenSslTool(
                "req", {
                    "-nodes": "",
                    "-new": "",
                    "-newkey": "rsa:2048",
                    "-x509": "",
                    "-keyout": keyFile,
                    "-out": certFile,
                    "-days": 365,
                    "-subj": "'/CN=myclusterxxx.hwx.site/OU=HWX/O=QE/L=Santa Clara/ST=CA/C=US'"
                }
            )
            assert exit_code == 0, "Failed to generate CA"

            clientTruststoreFile = os.path.join(storedir, "truststore.jks")
            exit_code, _output = cls.runKeyTool(
                "import", {
                    "-keystore": clientTruststoreFile,
                    "-alias": "CARoot",
                    "-file": certFile,
                    "-storepass": "password",
                    "-noprompt": ""
                }
            )
            assert exit_code == 0, "Failed to add CA to clients trustore"

            serverTruststoreFile = os.path.join(storedir, "server.truststore.jks")
            exit_code, _output = cls.runKeyTool(
                "import", {
                    "-keystore": serverTruststoreFile,
                    "-alias": "CARoot",
                    "-file": certFile,
                    "-storepass": "password",
                    "-noprompt": ""
                }
            )
            assert exit_code == 0, "Failed to add CA to servers trustore"

        for host in hosts:
            keystoreFile = os.path.join(storedir, "keystore_%s.jks" % host)
            exit_code, _output = cls.runKeyTool(
                "genkey", {
                    "-alias": "example.com_%s" % host,
                    "-keyalg": "RSA",
                    "-storetype": "JKS",
                    "-keysize": "2048",
                    "-storepass": "password",
                    "-keypass": "password",
                    "-keystore": keystoreFile,
                    "-dname": "\"CN=%s, OU=HWX, O=QE, L=Santa Clara, ST=CA, C=US\"" % host
                }
            )
            assert exit_code == 0, "Failed to generate key"
            if certify_key:
                certReqFile = os.path.join(storedir, '%s-cert-file' % host)
                exit_code, _output = cls.runKeyTool(
                    "certreq", {
                        "-keystore": keystoreFile,
                        "-alias": "example.com_%s" % host,
                        "-file": certReqFile,
                        "-storepass": "password",
                        "-keypass": "password"
                    }
                )
                assert exit_code == 0, "Failed to generate certificate sign request"

                certOutFile = os.path.join(storedir, '%s-cert-signed' % host)
                exit_code, _output = cls.runOpenSslTool(
                    "x509", {
                        "-req": "",
                        "-CA": certFile,
                        "-CAkey": keyFile,
                        "-in": certReqFile,
                        "-out": certOutFile,
                        "-days": 10,
                        "-CAcreateserial": "",
                        "-passin": "pass:password"
                    }
                )

                exit_code, _output = cls.runKeyTool(
                    "import", {
                        "-keystore": keystoreFile,
                        "-alias": "CARoot",
                        "-file": certFile,
                        "-storepass": "password",
                        "-noprompt": ""
                    }
                )
                assert exit_code == 0, "Failed to import CA"

                exit_code, _output = cls.runKeyTool(
                    "import", {
                        "-keystore": keystoreFile,
                        "-alias": "example.com_%s" % host,
                        "-file": certOutFile,
                        "-storepass": "password",
                        "-noprompt": ""
                    }
                )
                assert exit_code == 0, "Failed to import certificate"
            else:
                certFile = os.path.join(storedir, "%s.crt" % host)
                exit_code, _output = cls.runKeyTool(
                    "export", {
                        "-alias": "example.com_%s" % host,
                        "-storepass": "password",
                        "-keystore": keystoreFile,
                        "-file": certFile
                    }
                )
                assert exit_code == 0, "Failed to export certificate"
                truststoreFile = os.path.join(storedir, "truststore.jks")
                exit_code, _output = cls.runKeyTool(
                    "import", {
                        "-trustcacerts": "",
                        "-alias": "example.com_%s" % host,
                        "-noprompt": "",
                        "-storepass": "password",
                        "-keystore": truststoreFile,
                        "-file": certFile
                    }
                )
                assert exit_code == 0, "Failed to import certificate"
