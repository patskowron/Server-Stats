import subprocess
import os


modules = {'python': 'apps/python/2.7', 'java': 'apps/java/1.7', 'gatk': 'apps/binapps/GATK', 'R': 'apps/R/3.2.1'}


def float2qsubtime(time_float):
    hours = str(int(time_float))
    minutes = str(int((time_float - int(time_float)) * 60))
    if len(minutes) == 1:
        minutes = '0' + minutes
    return hours + ':' + minutes + ':00'


def q_script(cmd, out, mo='NONE', t=8.0, vmem=2, mem=6, hold='NONE', jid='DEFAULT', 
             tr=1, evolgen=False, node='0', array='no_array', environment='~/.bash_profile'):

    """
    function that prints a bash script suitable for submission to the son of grid engine, using qsub

    :param cmd: list
    :param out: str
    :param mo: list
    :param t: float
    :param vmem: int
    :param mem: int
    :param hold: list
    :param jid: str
    :param tr: int
    :param evolgen: bool
    :param node: str
    :param array: list
    :return:
    """

    # check options
    if type(cmd) is not list:
        raise TypeError('cmd must be a list')

    if type(cmd[0]) is not str or type(node) is not str:
        raise TypeError('entries in cmd list and node must be str')

    if not mo == 'NONE':
        if type(mo) is not list:
            raise TypeError('mo must be a list')
        for m in mo:
            if m not in modules.keys():
                raise KeyError(m + ' not a valid module')

    if not hold == 'NONE':
        if type(hold) is not list:
            raise TypeError('hold must be a list')

    if type(t) is int:
        t = float(t)

    if type(t) is not float:
        raise TypeError('t must be a float')

    if type(vmem) is not int or type(mem) is not int \
            or type(tr) is not int:

        raise TypeError('vmem, mem, tr and node must be integers')

    if array != 'no_array' and type(array) is not list:
        raise TypeError('array must be a list')

    # set variables
    run_time = '#PBS -l walltime=' + float2qsubtime(t) + '\n'
    memory = '#PBS -l mem='+str(mem)+'g\n#PBS -l vmem='+str(vmem)+'g\n'
    file_pos = out.rfind('/')+1  # identifies position of file name in path string
    if jid == 'DEFAULT':
        output_name = out[0:file_pos] + out[file_pos:] + '_job.sh'
    else:
        output_name = out[0:file_pos] + jid
    outs = '\n#PBS -N ' + output_name[output_name.rfind('/')+1:] + '\n#PBS -o '+out+'.out\n#PBS -e '+out+'.error\n'
    out_dir_path = out[:out.rfind('/') + 1]
    if not os.path.isdir(out_dir_path):
        os.makedirs(out_dir_path)
    node_str = ''
    if node != '0':
        node_str = '#PBS -l nodes=' + node + '\n'

    # construct shell contents
    shell_contents = '#!/bin/bash -x\n\n'
    if not mo == 'NONE':
        for m in mo:
            shell_contents += 'module load  ' + modules[m] + '\n'
    if array != 'no_array':
        shell_contents += '\n#PBS -t ' + str(array[0]) + '-' + str(array[1]) + '\n'
    shell_contents += '\n' + run_time + memory + '\n'
    if tr != 1:
        shell_contents += '#PBS -pe openmp ' + str(tr) + '\n'
    if evolgen is True:
        shell_contents += '#PBS -P evolgen\n#PBS -q evolgen.q\n'
    shell_contents += node_str
    shell_contents += outs + "\n"
    if hold is not 'NONE':
        hold = ','.join(hold)
        shell_contents += '#PBS -hold_jid ' + hold + '\n\n'
    
    shell_contents +='source ' + environment + '\n\n'
    
    for x in cmd:
        shell_contents += x + '\n'

    # output shell script string
    return shell_contents, output_name


def q_print(cmd, out, mo='NONE', t=8.0, vmem=2, mem=6, hold='NONE', jid='DEFAULT', tr=1, evolgen=False,
            node='0', array='no_array', environment='~/.bash_profile'):

    """
    function that prints a bash script suitable for submission to the son of grid engine, using qsub

    :param cmd: list
    :param out: str
    :param mo: list
    :param t: float
    :param vmem: int
    :param mem: int
    :param hold: list
    :param jid: str
    :param tr: int
    :param evolgen: bool
    :param node: str
    :param array: list
    :return:
    """

    script = q_script(cmd, out, mo=mo, t=t, vmem=vmem, mem=mem, hold=hold, jid=jid, 
                      tr=tr, evolgen=evolgen, node=node, array=array, environment=environment)[0]

    print(script)


def q_write(cmd, out, mo='NONE', t=8.0, vmem=2, mem=6, hold='NONE', jid='DEFAULT', tr=1, evolgen=False,
            node='0', array='no_array', environment='~/.bash_profile'):

    """
    function that writes a bash script suitable for submission to the son of grid engine, using qsub

    :param cmd: list
    :param out: str
    :param mo: list
    :param t: float
    :param vmem: int
    :param mem: int
    :param hold: list
    :param jid: str
    :param tr: int
    :param evolgen: bool
    :param node: int
    :param array: list
    :return:
    """

    script_data = q_script(cmd, out, mo=mo, t=t, vmem=vmem, mem=mem, hold=hold, jid=jid, 
                           tr=tr, evolgen=evolgen, node=node, array=array, environment=environment)

    script = script_data[0]
    output_name = script_data[1]

    # output shell script
    output = open(output_name, 'w')
    output.write(script)
    output.close()


def q_sub(cmd, out, mo='NONE', t=8, vmem=2, mem=6, hold='NONE', jid='DEFAULT', tr=1, evolgen=False,
          node='0', array='no_array', environment='~/.bash_profile'):

    """
    function that writes and submits a bash script to the son of grid engine, using qsub

    :param cmd: list
    :param out: str
    :param mo: list
    :param t: float
    :param vmem: int
    :param mem: int
    :param hold: list
    :param jid: str
    :param tr: int
    :param evolgen: bool
    :param node: str
    :param array: list
    :return:
    """

    script_data = q_script(cmd, out, mo=mo, t=t, vmem=vmem, mem=mem, hold=hold, jid=jid, 
                           tr=tr, evolgen=evolgen, node=node, array=array, environment=environment)

    script = script_data[0]
    output_name = script_data[1]

    # output shell script
    output = open(output_name, 'w')
    output.write(script)
    output.close()

    # submit script
    subprocess.call('qsub ' + output_name, shell=True)