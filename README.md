Scope:
	A `half decent` program to backup and restore a given system to desired state
Restoration should be done without internet connection.

################################################################################

Assumptions:
 - System should have Debian 7 or equivalent distros of it.
 - Config files should be parsable by Python's ConfigParser.
 - Backup/restoration will be done to/from a device directly connected to the machine as a disk.
 - Backup machine should be connected to the internet or appropriate repository.
 - While backup is being done, the backup files will not get corrupted/deleted.

################################################################################

Program's high level description:

0) Check whether it's 1st run or not.
1) Generate the initial queue of tasks (task_queue), if not done earlier.
2) Execute tasks from the task_queue and update the task_queue accordingly.
3) If finished, exit.

################################################################################

Files created by script:
 - task_queue
 - queue_head
 - get_backup_of_desired_state_machine.log.`TIMESTAMP`      (log file)
 - wget.log (if backup_globally_installed_packages = True)  (log file)

################################################################################

Tasks:
 - get_dpkg_selections
 - copy_tar_files
 - generate_urls
 - download_packages
 - write_names_of_locally_installed_packages
 - make_tar

################################################################################
