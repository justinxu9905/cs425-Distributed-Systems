import matplotlib.pyplot as plt
import numpy as np
import statistics
import sys
from collections import defaultdict


# get the map of delay time
def process():
    f = open("3_node_delay_data.txt", "r")
    line = f.readline()
    delay_dic = defaultdict(list)
    time_step = 1
    lst = []

    while line:
        # convert every line to an array
        line_lst = line.split(" ")
        # remove the /n
        for i in range(len(line_lst)):
            line_lst[-1] = str(line_lst[-1]).replace('\n', '')
        # add all to one list
        all_num = ' '.join(line_lst)
        lst = [all_num]

        # if nothing
        if lst[0] == '':
            delay_dic[time_step].append(0)
            time_step += 1
        # else store
        else:
            i = lst[0].split(" ")
            delay_dic[time_step] += [float(s) for s in i]
            time_step += 1

        line = f.readline()

    print(delay_dic)
    return delay_dic


delay_dic = process()


# graph the min
def delay_time_plot_min(delay_dic):
    minimum, time = [], []
    for k, v in delay_dic.items():
        time.append(k)
        minimum.append(min(v))
    plt.figure(figsize=(6.4, 4.8))
    plt.plot(time, minimum, 'o-', alpha=0.5, label='minimum', linewidth=2)
    plt.title('Min Delay vs. time plot for 3 Nodes')
    plt.xlabel('Time in seconds')
    plt.ylabel('Delay in seconds')
    plt.legend()
    plt.savefig("3_node_min_delay_time_graph.png", format="PNG", bbox_inches='tight')


delay_time_plot_min(delay_dic)


# graph the max
def delay_time_plot_max(delay_dic):
    maximum, time = [], []
    for k, v in delay_dic.items():
        time.append(k)
        maximum.append(min(v))
    plt.figure(figsize=(6.4, 4.8))
    plt.plot(time, maximum, 'o-', alpha=0.5, label='maximum', linewidth=2)
    plt.title('Max Delay vs. time plot for 3 Nodes')
    plt.xlabel('Time in seconds')
    plt.ylabel('Delay in seconds')
    plt.legend()
    plt.savefig("3_node_max_delay_time_graph.png", format="PNG", bbox_inches='tight')


delay_time_plot_max(delay_dic)


# graph the median
def delay_time_plot_med(delay_dic):
    median, time = [], []
    for k, v in delay_dic.items():
        time.append(k)
        median.append(statistics.median(v))
    plt.figure(figsize=(6.4, 4.8))
    plt.plot(time, median, 'o-', alpha=0.5, label='median', linewidth=2)
    plt.title('Median Delay vs. time plot for 3 Nodes')
    plt.xlabel('Time in seconds')
    plt.ylabel('Delay in seconds')
    plt.legend()
    plt.savefig("3_node_med_delay_time_graph.png", format="PNG", bbox_inches='tight')


delay_time_plot_med(delay_dic)


# graph the 90%
def delay_time_plot_90(delay_dic):
    ninety, time = [], []
    for k, v in delay_dic.items():
        time.append(k)
        ninety.append(np.percentile(v, 90))
    plt.figure(figsize=(6.4, 4.8))
    plt.plot(time, ninety, 'o-', alpha=0.5, label='ninety percentile', linewidth=2)
    plt.title('90% Delay vs. time plot for 3 Nodes')
    plt.xlabel('Time in seconds')
    plt.ylabel('Delay in seconds')
    plt.legend()
    plt.savefig("3_node_90_delay_time_graph.png", format="PNG", bbox_inches='tight')


delay_time_plot_90(delay_dic)
