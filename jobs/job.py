#!/usr/bin/python3

import subprocess
import os
import stat

class job:
    def __init__(self, runscript, jobtype):
        ### Type of job (currently we support LSF and SLURM ###
        self.jobtype = jobtype

        ### job filename ###
        self.runscript = runscript
        self.cmdline = self.runscript + ".cmd"

        ### SLURM default parameters ###
        self.jobname = ""
        self.timelimit = "0:10"
        self.queue = ""
        self.cwd = ""
        self.stdout = "slurm_job_%j.out"
        self.stdout_replace = False
        self.stderr = "slurm_job_%j.err"
        self.stderr_replace = False
        self.ntasks = 1
        self.nodes = 1
        self.mb_per_task = ""
        self.procs_per_node = 1
        self.cpus_per_task = 1
        self.qos = ""

        ### environment variables ###
        self.modules_load = []
        self.modules_unload = []
        self.job_env = {}

        ### command ###
        self.job_exec = ""
        self.job_args = []
        self.repetitions = 1

    def set_jobname(self, jobname):
        self.jobname = jobname

    def set_timelimit(self, hours, minutes):
        hours += int(minutes / 60)
        minutes = minutes % 60 
        if self.__job_is_lsf():
            self.timelimit = "{0}:{1}".format(hours, minutes)
        else:
            self.timelimit = "{0}:{1}:00".format(hours, minutes)

    def set_queue(self, queue):
        self.queue = queue

    def set_qos(self, qos):
        self.qos = qos

    def set_cwd(self, dir):
        self.cwd = dir

    def set_stdout(self, dir, replace = False):
        self.stdout = dir
        self.stdout_replace = replace

    def set_stderr(self, dir, replace = False):
        self.stderr = dir
        self.stderr_replace = replace

    """
    for MPI jobs ntasks is the number of MPI processes
    and for sequential executions it is the number of 
    cores
    """
    def set_nrtasks(self, ntasks):
        self.ntasks = ntasks

    def set_nrnodes(self, nodes):
        self.nodes = nodes

    """
    mem is in MBs
    """
    def set_mem_per_task(self, mem):
        self.mb_per_task = mem

    def set_procs_per_node(self, procs):
        self.procs_per_node = procs

    def set_cpus_per_task(self, cpus):
        self.cpus_per_task = cpus

    def set_modules(self, load, unload):
        self.modules_load = load
        self.modules_unload = unload

    def set_envar(self, var, value):
        self.job_env[var] = value

    def set_command(self, command, repetitions = 1):
        self.job_exec = command
        self.repetitions = repetitions

    def set_args(self, args):
        self.job_args = args

    """
    SLURM specific stuff
    """
    def __write_sbatch(self, fp, prefix, value):
        if value:
            fp.write(prefix.format(value))
            fp.write("\n")

    def __create_slurm_script(self):
        with open(self.runscript, 'w') as fp:
            fp.write("#!/bin/bash\n\n")

            #set environment
            fp.write("\n#Job environment\n")
            if self.modules_unload:
                unload = ' '.join(self.modules_unload)
                fp.write("module unload {0}\n".format(unload))

            if self.modules_load:
                load = ' '.join(self.modules_load)
                fp.write("module load {0}\n".format(load))

            for var in self.job_env.keys():
                fp.write("export {0}={1}\n".format(var, self.job_env[var]))

            fp.write("$@")

        st = os.stat(self.runscript)
        os.chmod(self.runscript, st.st_mode | stat.S_IEXEC)

        with open(self.cmdline, 'w') as fp:
            fp.write("#!/bin/bash\n\n")

            #set SLURM arguments
            fp.write("# SLURM configuration\n")
            self.__write_sbatch(fp, '#SBATCH --job-name={0}', self.jobname)
            self.__write_sbatch(fp, '#SBATCH --workdir={0}', self.cwd)
            self.__write_sbatch(fp, '#SBATCH --time={0}', self.timelimit)
            self.__write_sbatch(fp, '#SBATCH --ntasks={0}', self.ntasks)
            self.__write_sbatch(fp, '#SBATCH --nodes={0}', self.nodes)
            self.__write_sbatch(fp, '#SBATCH --cpus-per-task={0}', self.cpus_per_task)
            self.__write_sbatch(fp, '#SBATCH --output={0}', self.stdout)
            self.__write_sbatch(fp, '#SBATCH --error={0}', self.stderr)
            if self.queue == 'debug':
                self.__write_sbatch(fp, "#SBATCH --qos={0}", 'debug')
            if self.stdout_replace or self.stderr_replace:
                self.__write_sbatch(fp, '#SBATCH --open-mode={0}', 'truncate')
            else:
                self.__write_sbatch(fp, '#SBATCH --open-mode={0}', 'append')

            if self.qos:
                self.__write_sbatch(fp, "#SBATCH --qos={0}", self.qos) 

            #set executable
            fp.write("#Job command\n")
            for it in range(self.repetitions):
                if self.ntasks > 1:
                    fp.write("srun ")

                fp.write("{0} {1} ".format(self.runscript, self.job_exec))
                for arg in self.job_args:
                    fp.write("{0} ".format(arg))
                fp.write("\n")
        
    """
    LSF specific stuff
    """
    def __write_bsub(self, fp, prefix, value):
        if value:
            fp.write(prefix.format(value))
            fp.write("\n")

    def __create_lsf_script(self):
        with open(self.runscript, 'w') as fp:
            fp.write("#!/bin/bash\n\n")

            #set BSUB arguments
            fp.write("#LSF configuration\n")
            self.__write_bsub(fp, '#BSUB -J {0}', self.jobname)
            self.__write_bsub(fp, '#BSUB -q {0}', self.queue)
            self.__write_bsub(fp, '#BSUB -cwd {0}', self.queue)
            self.__write_bsub(fp, '#BSUB -W {0}', self.timelimit)
            self.__write_bsub(fp, '#BSUB -n {0}', self.ntasks)
            self.__write_bsub(fp, '#BSUB -M {0}', self.mb_per_task)
            self.__write_bsub(fp, '#BSUB -R "span[ptile={0}]"', self.procs_per_node)
            if self.stdout_replace:
                self.__write_bsub(fp, '#BSUB -oo {0}', self.stdout)
            else:
                self.__write_bsub(fp, '#BSUB -o {0}', self.stdout)

            if self.stderr_replace:
                self.__write_bsub(fp, '#BSUB -eo {0}', self.stderr)
            else:
                self.__write_bsub(fp, '#BSUB -e {0}', self.stderr)

            #set environment
            fp.write("\n#Job environment\n")
            if self.modules_unload:
                unload = ' '.join(self.modules_unload)
                fp.write("module unload {0}\n".format(unload))

            if self.modules_load:
                load = ' '.join(self.modules_load)
                fp.write("module load {0}\n".format(load))

            for var in self.job_env:
                fp.write("export {0}={1}\n".format(var[0], var[1]))

            #set executable
            fp.write("\n#Job command\n")
            for it in range(self.repetitions):
                if self.ntasks > 1:
                    fp.write("mpirun ")
                fp.write("{0} ".format(self.job_exec))
                for arg in self.job_args:
                    fp.write("{0} ".format(arg))
                fp.write("\n")


    def __job_is_slurm(self):
        return self.jobtype == 'slurm' or self.jobtype == 'SLURM'

    def __job_is_lsf(self):
        return self.jobtype == 'lsf' or self.jobtype == 'LSF'

    def __create_script(self):
        if self.__job_is_slurm():
            self.__create_slurm_script()
        elif self.__job_is_lsf():
            self.__create_lsf_script()
        else:
            print("Something is wrong: Unknown jobtype")
            sys.exit(1)

    def submit_job(self):
        self.__create_script()

        if self.__job_is_slurm():
            #subprocess.call(['sbatch', self.runscript])
            subprocess.call(['sbatch', self.cmdline])
        elif self.__job_is_lsf():
            with open(self.runscript, 'r') as self.stdin: 
                subprocess.call('bsub', stdin=self.stdin)
