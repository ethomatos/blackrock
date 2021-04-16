#!/usr/bin/python
from datadog import initialize, api, statsd
from argparse import ArgumentParser
import sys
import os
import json
import time
import re
import shutil

# set debug, metadata, summary printout and active flags
debug = False
ddebug = False
active = True
stage1dir = '/var/stage1'
stage2dir = '/var/stage2'

# Read in API keys from a file
# The file format should be like this:
# apiKey:123456789
# appKey:123456789
def getAPIkeys(file):
	apiKey = None
	appKey = None
	with open(file) as apis:
		for line in apis:
			line.rstrip()	
			tag = line.split(':')
			if (tag[0] == "apiKey"):
				apiKey = tag[1]
				if (debug):
					print("apiKey = " + apiKey, end="")
			elif (tag[0] == "appKey"):
				appKey = tag[1]
				if (debug):
					print("appKey = " + appKey, end="")
	return(apiKey, appKey)

### STAGE1 - identify files
def stage1():
	with os.scandir(stage1dir) as dir:
		for entry in dir:
			if not entry.name.startswith('.') and entry.is_file():
				### Identify vendor files
				if re.search("typea", entry.name):
					logger("stage:stage1 - found typea " + entry.name)
					stage2("typea", entry.name)
				elif re.search("typeb", entry.name):
					logger("stage:stage1 - found typeb " + entry.name)
					stage2("typeb", entry.name)
				elif re.search("typec", entry.name):
					logger("stage:stage1 - found typec " + entry.name)
					stage2("typec", entry.name)
				elif re.search("typed", entry.name):
					logger("stage:stage1 - found typed " + entry.name)
					stage2("typed", entry.name)
				elif re.search("typee", entry.name):
					logger("stage:stage1 - found typee " + entry.name)
					stage2("typee", entry.name)

### STAGE2 - process the files
def stage2(type, file):
	fqfn = stage1dir + "/" + file
	### Do something here more specific
	### as an example, each file will be read in to count lines
	lcount = 0
	with open(fqfn, 'r') as input:
		for line in input:	
			lcount += 1
	logger("stage:stage2 - processed " + fqfn + " which has " + str(lcount) + " lines")		
	metadata = ["stage2", file, fqfn, lcount, type]
	sendEvent(metadata)
	stage3(type, file)

### STAGE3 - copy the file into another directory
def stage3(type, file):
	sourcefile = stage1dir + "/" + file
	outputfile = file.replace("type", "output")
	destfile = stage2dir + "/" + outputfile
	shutil.copy(sourcefile, destfile)
	logger("stage:stage3 - copied " + sourcefile + " into " + destfile)
	metadata = ["stage3", file, destfile, type]
	sendEvent(metadata)

def fileToVendor(type):
	if type == "typea":
		return("vendor1")
	elif type == "typeb":
		return("vendor2")
	elif type == "typec":
		return("vendor3")
	elif type == "typed":
		return("vendor4")
	elif type == "typee":
		return("vendor5")

def fileToClient(type):
	if type == "typea":
		return("client1")
	elif type == "typeb":
		return("client2")
	elif type == "typec":
		return("client3")
	elif type == "typed":
		return("client4")
	elif type == "typee":
		return("client5")

### send events to Datadog
def sendEvent(metadata):
	if (metadata[0] == "stage2"):
		# metadata = ["stage2", file, fqfn, lcount, type]
		title = "Stage2 Processed " + metadata[1] 
		text = metadata[2] + " has been processed successfully with " + str(metadata[3]) + " lines"
		tag = "file:" + metadata[1]
		tags = ['project:blackrock', 'stage:stage2', 'owner:et']
		tags.append(tag)
		tag = "type:" + metadata[4]
		tags.append(tag)
		tag = "vendor:" + fileToVendor(metadata[4])
		tags.append(tag)
		api.Event.create(title=title, text=text, tags=tags)
	elif (metadata[0] == "stage3"):
		# metadata = ["stage3", file, destfile, type]
		title = "Stage3 Copied " + metadata[1] 
		text = metadata[1] + " has been copied successfully to " + metadata[2]
		tag = "file:" + metadata[2]
		tags = ['project:blackrock', 'stage:stage3', 'owner:et']
		tags.append(tag)
		tag = "client:" + fileToClient(metadata[3])
		tags.append(tag)
		api.Event.create(title=title, text=text, tags=tags)

### logging
def logger(message):
	logfile = stage2dir + "/log"
	with open(logfile, 'a') as log: 
		log.write(message + "\n")


# Main routine of the program
def main(apifile):
	# Initialize Datadog API
	(apiKey, appKey) = getAPIkeys(apifile)
	options = {
		'api_key':apiKey.rstrip(),
		'app_key':appKey.rstrip()
	}
	initialize(**options)

	if active:
		stage1()

# Only executed as a standalone program, not from an import from another program
if __name__ == "__main__":
	parser = ArgumentParser(description='Blackrock demo')
	parser.add_argument('-i', help='API file in format apiKey:123 newline appKey:123', required=True)
	args = parser.parse_args()
	apifile = args.i if args.i else 'api.txt'
	main(apifile)
