#!/opt/python27/bin/python

__author__ = 'zhuyund'


def jobGenerator(executable, arguments, log_file, err_file, out_file):
    job = "Universe=vanilla\nExecutable={0}\n\nArguments={1}\nLog={2}\nError={3}\nOutput={4}\n\nQueue\n\n".format(executable,
                                                                                                             arguments,
                                                                                                             log_file,
                                                                                                             err_file,
                                                                                                             out_file)
    return job

