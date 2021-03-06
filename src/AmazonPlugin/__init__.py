from core.modules.vistrails_module import Module, ModuleError
import core.modules
import core.modules.basic_modules
import core.modules.module_registry
import core.system
import gui.application

from PyQt4 import QtCore, QtGui, QtWebKit
from PyQt4.QtCore import *
from PyQt4.QtGui import *
from PyQt4.QtWebKit import *

import api
import sys
import pexpect
import re
import uuid
import time
import scheduler

version = "0.0.5"
name = "AmazonPlugin"
identifier = "edu.cmu.nasaproject.vistrails.amazonplugin"

class JobStatusViewer(QtGui.QWidget):

    def __init__(self, parent=None):

        QtGui.QWidget.__init__(self, parent)
        self.setWindowTitle('Job Status')
        gridLayout = QtGui.QGridLayout()
        self.setLayout(gridLayout)

        self.usage_label = QtGui.QLabel()
        self.usage_label.setTextFormat(QtCore.Qt.RichText)
        self.usage_label.setOpenExternalLinks(True)
        gridLayout.addWidget(self.usage_label, 1, 0)

    def updateStatus(self):

        # [The LOGIN function is directly copied from remoteLogin package] 
        def login( thePrompt, password ):
            theResult = thePrompt.expect( ['continue connecting',
                                           'assword:',
                                           pexpect.EOF] )

            # check if this is the first time we have tried to login to the server
            if theResult==0:
                print >> sys.stderr," [scpModule] login -- first time fingerprint"
                thePrompt.sendline( 'yes' )
                theResult = thePrompt.expect( ['continue connecting',
                                               'assword:',
                                               pexpect.EOF] )

            # respond to the result after potential fingerprint acceptance
            if theResult==0:
                print >> sys.stderr," [scpModule] login -- sanity failure -- first time fingerprint again"
                raise RuntimeError, "sanity failure -- fingerprint double check"
            elif theResult==2:
                print >> sys.stderr," [scpModule] login -- received EOF signal"
                raise RuntimeError, "login failure -- early EOF received"

            # otherwise process the password prompt
            elif theResult==1:
                print >> sys.stderr," [scpModule] login -- received password prompt"
                thePrompt.sendline( password )
                theResult = thePrompt.expect( ['assword:',pexpect.EOF] )

                # check the responses
                if theResult==0:
                    print >> sys.stderr," [scpModule] login -- failure denied password"
                    raise RuntimeError,"login failure -- denied username/password"
                else:
                    print >> sys.stderr," [scpModule] successful login..."

        self.usage_label.setText("Loading...")

        # login info
        username = loginWindow.username
        password = loginWindow.password

        # spawn the scp pexpect thread and login
        spawnLine_queue = "ssh " + username + "@ok.freya.cc \"" + "find /home/hecc/job_queue -type f | grep '/"+username+"_'" + "\""
        spawnLine_running = "ssh " + username + "@ok.freya.cc \"" + "find /home/hecc/running -type f | grep '/"+username+"_'" + "\""
        spawnLine_results = "ssh " + username + "@ok.freya.cc \"" + "find /home/hecc/results -type f | grep '/"+username+"_'" + "\""
        thePrompt_queue = pexpect.spawn( spawnLine_queue )
        thePrompt_running = pexpect.spawn( spawnLine_running )
        thePrompt_results = pexpect.spawn( spawnLine_results )
        login( thePrompt_queue, password )
        login( thePrompt_running, password )
        login( thePrompt_results, password )
        in_queue_jobs = thePrompt_queue.before.replace("/home/hecc/job_queue/", "")
        running_jobs = thePrompt_running.before.replace("/home/hecc/running/", "")
        done_results = thePrompt_results.before.split('\n')

        displayText = "In Queue:<br>"+in_queue_jobs+"<br>Running:<br>"+running_jobs+"<br>Done:<br>"
        for result in done_results[1:-1]:
            displayText += "[Job] "+result.replace("/home/hecc/results/", "")[:-5]+"<br>"
            spawnLine_result = "ssh " + username + "@ok.freya.cc \"" + "cat "+result[:-1] + "\""
            thePrompt_result = pexpect.spawn( spawnLine_result )
            login( thePrompt_result, password )
            displayText += '<a href="%s">%s</a><br>' %(thePrompt_result.before[:-1], thePrompt_result.before[:-1])
        
        self.usage_label.setText(displayText)


class LoginViewer(QtGui.QWidget):

    def __init__(self, parent=None):
        QtGui.QWidget.__init__(self, parent)
        self.setWindowTitle('Login Viewer')
        gridLayout = QtGui.QGridLayout()
        self.setLayout(gridLayout)

        self.usernameLabel = QtGui.QLabel('User Name')
        gridLayout.addWidget(self.usernameLabel, 0, 0)
        self.usernameEdit = QtGui.QLineEdit()
        gridLayout.addWidget(self.usernameEdit, 0, 1)

        self.passwordLabel = QtGui.QLabel('Password')
        gridLayout.addWidget(self.passwordLabel, 1, 0)
        self.passwordEdit = QtGui.QLineEdit()
        self.passwordEdit.setEchoMode(QtGui.QLineEdit.Password)
        gridLayout.addWidget(self.passwordEdit, 1, 1)

        self.loginButton = QtGui.QPushButton('Login')
        gridLayout.addWidget(self.loginButton, 2, 1)
        self.connect(self.loginButton, QtCore.SIGNAL('clicked()'), self.save)

    def save(self):
        self.username = str(self.usernameEdit.text())
        self.password = str(self.passwordEdit.text())
        self.hide()

class EstimateViewer(QtGui.QWidget):

    def __init__(self, parent=None):
        QtGui.QWidget.__init__(self, parent)
        self.setWindowTitle('Node Configuration Panel')
        gridLayout = QtGui.QGridLayout()
        self.setLayout(gridLayout)

        self.timeLabel = QtGui.QLabel('General Purpose-C')
        gridLayout.addWidget(self.timeLabel, 1, 1)
        self.timeLabel = QtGui.QLabel('General Purpose-P')
        gridLayout.addWidget(self.timeLabel, 1, 2)
        self.timeLabel = QtGui.QLabel('Compute Optimized')
        gridLayout.addWidget(self.timeLabel, 1, 3)
        self.timeLabel = QtGui.QLabel('Memory Optimized')
        gridLayout.addWidget(self.timeLabel, 1, 4)

        self.prefCombo = QtGui.QComboBox()
        self.prefCombo.addItem("Select", "select")
        self.prefCombo.addItem("Performance Efficient", "performance")
        self.prefCombo.addItem("Cost Efficient", "cost")
        self.prefCombo.addItem("Manually Set", "manual")
        gridLayout.addWidget(self.prefCombo, 2, 0)
        self.connect(self.prefCombo, QtCore.SIGNAL('currentIndexChanged(QString)'), self.changeNodeInputs)
        
        # Sandy bridge input
        self.sanEdit = QtGui.QLineEdit()
        self.sanEdit.setText("0")
        gridLayout.addWidget(self.sanEdit, 2, 1)
        self.wesEdit = QtGui.QLineEdit()
        self.wesEdit.setText("0")
        gridLayout.addWidget(self.wesEdit, 2, 2)
        self.nehEdit = QtGui.QLineEdit()
        self.nehEdit.setText("0")
        gridLayout.addWidget(self.nehEdit, 2, 3)
        self.harEdit = QtGui.QLineEdit()
        self.harEdit.setText("0")
        gridLayout.addWidget(self.harEdit, 2, 4)
        
        self.estiButton = QtGui.QPushButton('Estimate')
        gridLayout.addWidget(self.estiButton, 4, 4)
        self.connect(self.estiButton, QtCore.SIGNAL('clicked()'), self.estimate)
        
        self.osCombo = QtGui.QComboBox()
        self.osCombo.addItem("Linux/UNIX", "linux")
        self.osCombo.addItem("Windows", "windows")
        gridLayout.addWidget(self.osCombo, 5, 0)
        self.connect(self.osCombo, QtCore.SIGNAL('currentIndexChanged(QString)'), self.changeOsInputs)

        # Node Cost
        self.sancostLabel = QtGui.QLabel('$0.495/hour')
        gridLayout.addWidget(self.sancostLabel, 5, 1)
        self.wescostLabel = QtGui.QLabel('$0.52/hour')
        gridLayout.addWidget(self.wescostLabel, 5, 2)
        self.nehcostLabel = QtGui.QLabel('$0.66/hour')
        gridLayout.addWidget(self.nehcostLabel, 5, 3)
        self.harcostLabel = QtGui.QLabel('$0.46/hour')
        gridLayout.addWidget(self.harcostLabel, 5, 4)
       
         # Estimate Cost
        self.costLabel = QtGui.QLabel('Total usage:')
        gridLayout.addWidget(self.costLabel, 6, 0)
        self.costEdit = QtGui.QLineEdit()
        #self.costEdit.setText("$%d"%scheduler.get_cost())
        self.costEdit.setText("$ 0")
        gridLayout.addWidget(self.costEdit, 6, 1)
        
        self.loginButton = QtGui.QPushButton('Submit')
        gridLayout.addWidget(self.loginButton, 11, 4)


    def changeNodeInputs(self):
      # preference = self.prefCombo.currentText()
      preference = self.prefCombo.itemData(self.prefCombo.currentIndex()).toString()
      print >> sys.stderr, preference

      sc = scheduler.Scheduler()
      if preference == "performance":
        self.sanEdit.setText("4")
        self.wesEdit.setText("2")
        self.nehEdit.setText("1")
        self.harEdit.setText("1")
        self.costEdit.setText("$ %d"%sc.get_cost(4,2,1,1))
        self.timeEdit.setText("120 minutes")
      elif preference == "cost":
        self.sanEdit.setText("1")
        self.wesEdit.setText("1")
        self.nehEdit.setText("2")
        self.harEdit.setText("4")
        self.costEdit.setText("$ %d"%sc.get_cost(1,1,2,4))
        self.timeEdit.setText("265 minutes")
      elif preference == "manual":
        self.sanEdit.setText("0")
        self.wesEdit.setText("0")
        self.nehEdit.setText("0")
        self.harEdit.setText("0")
        self.costEdit.setText("$ %d"%sc.get_cost(0,0,0,0))
        self.timeEdit.setText("0")
 
    def changeOsInputs(self):
      # preference = self.prefCombo.currentText()
      preference = self.osCombo.itemData(self.osCombo.currentIndex()).toString()
      print >> sys.stderr, preference

      if preference == "linux":
        self.sancostLabel.setText("$0.495/hour")
        self.wescostLabel.setText("$0.52/hour")
        self.nehcostLabel.setText("$0.66/hour")
        self.harcostLabel.setText("$0.46/hour")
      elif preference == "windows":
        self.sancostLabel.setText("$0.747/hour")
        self.wescostLabel.setText("$0.768/hour")
        self.nehcostLabel.setText("$0.98/hour")
        self.harcostLabel.setText("$0.56/hour")
    
   
    def estimate(self):
        sc = scheduler.Scheduler()
        self.costEdit.setText("$ %d"%sc.get_cost(4,2,1,1))
        self.timeEdit.setText("360 minutes")


class SendViewer(QtGui.QWidget):

    def __init__(self, parent=None):
        QtGui.QWidget.__init__(self, parent)
        self.setWindowTitle('Send to Amazon Settings')
        gridLayout = QtGui.QGridLayout()
        self.setLayout(gridLayout)

        self.emailLabel = QtGui.QLabel('Notification Email')
        gridLayout.addWidget(self.emailLabel, 0, 0)
        self.emailEdit = QtGui.QLineEdit()
        gridLayout.addWidget(self.emailEdit, 0, 1)

        """
        self.ncpusLabel = QtGui.QLabel('Number of CPUs')
        gridLayout.addWidget(self.ncpusLabel, 1, 0)
        self.ncpusEdit = QtGui.QLineEdit()
        self.ncpusEdit.setText("32")
        gridLayout.addWidget(self.ncpusEdit, 1, 1)
        """

        self.prefLabel = QtGui.QLabel('Preference')
        gridLayout.addWidget(self.prefLabel, 2, 0)
        self.prefCombo = QtGui.QComboBox()
        self.prefCombo.addItem("Performance", "performance")
        self.prefCombo.addItem("Cost", "cost")
        self.prefCombo.addItem("Manual", "manual")
        gridLayout.addWidget(self.prefCombo, 2, 1)
        self.connect(self.prefCombo, QtCore.SIGNAL('currentIndexChanged(QString)'), self.changeNodeInputs)

        """
        self.nodeLabel = QtGui.QLabel('Node Type')
        gridLayout.addWidget(self.nodeLabel, 3, 0)
        self.nodeCombo = QtGui.QComboBox()
        self.nodeCombo.addItem("Sandy Bridge", "san")
        self.nodeCombo.addItem("Westmere", "wes")
        self.nodeCombo.addItem("Nehalem", "neh")
        self.nodeCombo.addItem("Harpertown", "har")
        gridLayout.addWidget(self.nodeCombo, 3, 1)

        self.selectLabel = QtGui.QLabel('Number of Nodes')
        gridLayout.addWidget(self.selectLabel, 4, 0)
        self.selectEdit = QtGui.QLineEdit()
        self.selectEdit.setText("2")
        gridLayout.addWidget(self.selectEdit, 4, 1)
        """

        # Sandy bridge input
        self.sanLabel = QtGui.QLabel('Sandy Bridge')
        gridLayout.addWidget(self.sanLabel, 3, 0)
        self.sanEdit = QtGui.QLineEdit()
        self.sanEdit.setText("4")
        gridLayout.addWidget(self.sanEdit, 3, 1)

        # Sandy bridge input
        self.wesLabel = QtGui.QLabel('Westmere')
        gridLayout.addWidget(self.wesLabel, 4, 0)
        self.wesEdit = QtGui.QLineEdit()
        self.wesEdit.setText("2")
        gridLayout.addWidget(self.wesEdit, 4, 1)

        # Sandy bridge input
        self.nehLabel = QtGui.QLabel('Nehalem')
        gridLayout.addWidget(self.nehLabel, 5, 0)
        self.nehEdit = QtGui.QLineEdit()
        self.nehEdit.setText("1")
        gridLayout.addWidget(self.nehEdit, 5, 1)

        # Sandy bridge input
        self.harLabel = QtGui.QLabel('Harpertown')
        gridLayout.addWidget(self.harLabel, 6, 0)
        self.harEdit = QtGui.QLineEdit()
        self.harEdit.setText("1")
        gridLayout.addWidget(self.harEdit, 6, 1)

        self.sendButton = QtGui.QPushButton('Send to Amazon')
        gridLayout.addWidget(self.sendButton, 8, 1)
        self.connect(self.sendButton, QtCore.SIGNAL('clicked()'), self.send)

    def changeNodeInputs(self):
      # preference = self.prefCombo.currentText()
      preference = self.prefCombo.itemData(self.prefCombo.currentIndex()).toString()
      print >> sys.stderr, preference

      if preference == "performance":
        self.sanEdit.setText("4")
        self.wesEdit.setText("2")
        self.nehEdit.setText("1")
        self.harEdit.setText("1")
      elif preference == "cost":
        self.sanEdit.setText("1")
        self.wesEdit.setText("1")
        self.nehEdit.setText("2")
        self.harEdit.setText("4")
      elif preference == "manual":
        self.sanEdit.setText("0")
        self.wesEdit.setText("0")
        self.nehEdit.setText("0")
        self.harEdit.setText("0")

    def send(self):
    
        # [The LOGIN function is directly copied from remoteLogin package] 
        # LOGIN -- handle the login with all its odd 'expected' cases of
        # failure/success.  returns a boolean indicating the ability to
        # login to the prompt with the given username and password
        def login( thePrompt, password ):
            theResult = thePrompt.expect( ['continue connecting',
                                           'assword:',
                                           pexpect.EOF] )

            # check if this is the first time we have tried to login to the server
            if theResult==0:
                print >> sys.stderr," [scpModule] login -- first time fingerprint"
                thePrompt.sendline( 'yes' )
                theResult = thePrompt.expect( ['continue connecting',
                                               'assword:',
                                               pexpect.EOF] )

            # respond to the result after potential fingerprint acceptance
            if theResult==0:
                print >> sys.stderr," [scpModule] login -- sanity failure -- first time fingerprint again"
                raise RuntimeError, "sanity failure -- fingerprint double check"
            elif theResult==2:
                print >> sys.stderr," [scpModule] login -- received EOF signal"
                raise RuntimeError, "login failure -- early EOF received"

            # otherwise process the password prompt
            elif theResult==1:
                print >> sys.stderr," [scpModule] login -- received password prompt"
                thePrompt.sendline( password )
                theResult = thePrompt.expect( ['assword:',pexpect.EOF] )

                # check the responses
                if theResult==0:
                    print >> sys.stderr," [scpModule] login -- failure denied password"
                    raise RuntimeError,"login failure -- denied username/password"
                else:
                    print >> sys.stderr," [scpModule] successful login..."

    
        # this line prints out the latest workflow name which we can leverage later
        workflow_name = api.get_available_versions()[1][api.get_available_versions()[0][-1]]
        
        # login info
        username = loginWindow.username
        password = loginWindow.password
        
        vt_filepath = api.get_current_controller().get_locator().name
        remote_filename = username + "_" + str(uuid.uuid4()) + "_" + vt_filepath.split('/')[-1][:-3]

        # spawn the scp pexpect thread and login
        config_text = "email: "+str(self.emailEdit.text())+"\\nworkflow_name: "+str(workflow_name)+"\\nscheduling: "
        config_text += "\\n    type: "+str(self.prefCombo.itemData(self.prefCombo.currentIndex()).toString())
        """
        config_text += "\\n    ncpus: "+str(self.ncpusEdit.text())
        config_text += "\\n    node: "+str(self.nodeCombo.itemData(self.nodeCombo.currentIndex()).toString())
        config_text += "\\n    select: "+str(self.selectEdit.text())
        """
        #print >> sys.stderr, config_text

        spawnLine_cfg = "ssh " + username + "@ok.freya.cc \"" + "echo -ne '"+config_text+"' >> /home/hecc/config/"+remote_filename+".yml" + "\""
        #print >> sys.stderr, spawnLine_cfg
        thePrompt_cfg = pexpect.spawn( spawnLine_cfg )
        login( thePrompt_cfg, password )

        spawnLine = "scp " + vt_filepath + " " + username + "@ok.freya.cc:/home/hecc/job_queue/" + remote_filename + ".vt"
        #print >> sys.stderr," [scp Module] scp spawning line: (" + spawnLine + ")"
        thePrompt = pexpect.spawn( spawnLine )
        login( thePrompt, password )

        self.hide()

class AmazonPlugin(Module):
    """AmazonPlugin is an adapter to Amazon"""

    def __init__( self ):
        Module.__init__(self)

    def is_cacheable(self):
        return False

    def compute(self):
        print >> sys.stderr," Compute "
        # grab input information from the ports
        #self.vt_filepath = self.forceGetInputFromPort( "vt_filepath" )
        #self.remote_filename = self.forceGetInputFromPort( "remote_filename" )
        #self.username = self.forceGetInputFromPort( "username" )
        #self.password = self.forceGetInputFromPort( "password" )
        #self.sendMode = self.forceGetInputFromPort( "send mode", True )

        # flag the operation as completed
        #self.setResult( "complete flag", True )

###############################################################################

def initialize(*args, **keywords):

    # We'll first create a local alias for the module registry so that
    # we can refer to it in a shorter way.
    basic = core.modules.basic_modules
    reg = core.modules.module_registry.registry
    reg.add_module(AmazonPlugin)
    
    global loginWindow, jobstatusWindow, sendWindow, webWindow, estimateWindow
    loginWindow = LoginViewer()
    jobstatusWindow = JobStatusViewer()
    sendWindow = SendViewer()
    webWindow = QWebView()
    estimateWindow = EstimateViewer()

###################

def menu_items():

    def view_usages():
        usageWindow.show()
        usageWindow.activateWindow()
        usageWindow.raise_()

    def view_cpu_usage():
        webWindow.load(QUrl("http://www.nas.nasa.gov/monitoring/hud/realtime/pleiadespanel1.html"))
        webWindow.resize(350,540)
        webWindow.show()
        webWindow.activateWindow()
        webWindow.raise_()

    def view_pbs_status():
        webWindow.load(QUrl("http://www.nas.nasa.gov/monitoring/hud/realtime/pleiadespanel2.html"))
        webWindow.resize(280,380)
        webWindow.show()
        webWindow.activateWindow()
        webWindow.raise_()

    def view_filesystem_usage():
        webWindow.load(QUrl("http://www.nas.nasa.gov/monitoring/hud/realtime/pleiadespanel3.html"))
        webWindow.resize(320,450)
        webWindow.show()
        webWindow.activateWindow()
        webWindow.raise_()

    def view_jobstatus():
        jobstatusWindow.show()
        jobstatusWindow.activateWindow()
        jobstatusWindow.raise_()
        jobstatusWindow.updateStatus()

    def log_on_Amazon():
        loginWindow.show()
        loginWindow.activateWindow()
        loginWindow.raise_()

    def view_cost_estimation():
        estimateWindow.show()
        estimateWindow.activateWindow()
        estimateWindow.raise_()

    def send_to_Amazon():
        sendWindow.show()
        sendWindow.activateWindow()
        sendWindow.raise_()

    lst = []
    lst.append(("Log on Amazon", log_on_Amazon))
    lst.append(("Send to Amazon", send_to_Amazon))
    lst.append(("View CPU Usage", view_cpu_usage))
    lst.append(("View Cost Estimation", view_cost_estimation))
    lst.append(("View PBS Status", view_pbs_status))
    lst.append(("View File System Status", view_filesystem_usage))
    lst.append(("View Job Status", view_jobstatus))
    return tuple(lst)
