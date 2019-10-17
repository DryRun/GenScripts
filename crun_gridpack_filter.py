#!/usr/bin/env python
# Run gridpack to NANO on condor

import os
import sys

if __name__ == "__main__":
	import argparse
	parser = argparse.ArgumentParser(description="Run gridpack to NANO on condor")
	parser.add_argument("name", type=str, help="Name of output folder (will be created in current directory)")
	parser.add_argument("gridpack", type=str, help="Path to gridpack")
	parser.add_argument("--nevents_job", type=int, default=100, help="Number of events per job")
	parser.add_argument("--njobs", type=int, default=1, help="Number jobs")
	parser.add_argument("--keepMini", action='store_true', help="Keep MiniAOD")
	parser.add_argument("--keepDR", action='store_true', help="Keep DR")
	parser.add_argument("--keepGS", action='store_true', help="Keep GS")
	args = parser.parse_args()

	# Check gridpack exists
	gridpack_abspath = os.path.abspath(args.gridpack)
	if not os.path.isfile(gridpack_abspath):
		raise ValueError("Couldn't find gridpack at {}".format(gridpack_abspath))
	print "Using gridpack at {}".format(gridpack_abspath)

	# Create and move to working directory
	if os.path.isdir(args.name):
		raise ValueError("Output directory {} already exists!".format(args.name))
	os.system("mkdir -pv {}".format(args.name))
	cwd = os.getcwd()
	os.chdir(args.name)

	# Submit to condor
	with open("run.sh", 'w') as run_script:
		run_script.write("#!/bin/bash\n")
		run_script.write("ls -lrth\n")
		run_script.write("pwd\n")
		#run_script.write("env\n")
		command = "source run_gridpack_filter.sh $_CONDOR_SCRATCH_DIR/{} {} {} $1 2>&1 ".format(
			os.path.basename(gridpack_abspath), 
			args.name, 
			args.nevents_job
		)
		run_script.write(command + "\n")
		run_script.write("mv *py $_CONDOR_SCRATCH_DIR\n")
		run_script.write("mv *NanoAOD*root $_CONDOR_SCRATCH_DIR\n")
		if args.keepMini:
			run_script.write("mv *MiniAOD*root $_CONDOR_SCRATCH_DIR\n")
		if args.keepDR:
			run_script.write("mv *DR*root $_CONDOR_SCRATCH_DIR\n")
		if args.keepGS:
			run_script.write("mv *GS*root $_CONDOR_SCRATCH_DIR\n")
		run_script.write("ls -lrth\n")
		run_script.write("pwd\n")

	files_to_transfer = [gridpack_abspath, "/afs/cern.ch/user/d/dryu/DAZSLE/gen/scripts/run_gridpack_filter.sh"]
	csub_command = "csub run.sh -t tomorrow -F {} --queue_n {}".format(",".join(files_to_transfer), args.njobs) # --os SLCern6
	os.system(csub_command)

	os.chdir(cwd)