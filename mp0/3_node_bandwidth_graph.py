import matplotlib.pyplot as plt

# bandwidth_time_graph
with open('3_node_bandwidth_data.txt', 'r') as f:
    arr = []
    # iterate every time step
    for i in f:
        arr.append(int(i))
    plt.plot(arr)
    plt.title('Bandwidth plot for 3 Nodes')
    plt.ylabel("Bandwidth in bits")
    plt.xlabel("Time in seconds")
    # save the png
    plt.savefig("3_node_bandwidth_time_graph.png", format="PNG", bbox_inches='tight')
