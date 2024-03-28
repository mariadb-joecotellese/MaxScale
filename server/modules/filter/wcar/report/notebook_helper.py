#
# Copyright (c) 2024 MariaDB plc
#
import matplotlib.pyplot as plt
from os import environ as env
import maxpostprocess as pp
import math
import json
from hashlib import sha1
from IPython.display import Markdown, HTML, display
import ipywidgets as widgets


def load_data():
    if "CANONICALS" in env and "BASELINE_REPLAY" in env and "COMPARISON_REPLAY" in env:
        file1 = env["BASELINE_REPLAY"]
        file2 = env["COMPARISON_REPLAY"]
        res1 = pp.process(env["BASELINE_REPLAY"], env["CANONICALS"])
        res2 = pp.process(env["COMPARISON_REPLAY"], env["CANONICALS"])
    elif "BASELINE" in env and "COMPARISON" in env:
        file1 = env["BASELINE"]
        with open(file1) as f:
            res1 = json.load(f)
        file2 = env["COMPARISON"]
        with open(file2) as f:
            res2 = json.load(f)
    else:
        raise Exception("Either a WCAR replay or a DIFF summary must be given via environment variables.")

    compared = pp.compare(res1, res2)

    display(Markdown(f"**Baseline**: {file1}"))
    display(Markdown(f"**Comparison**: {file2}"))
    return compared, res1, res2


def plot_qps(res1, res2):
    fig, ax = plt.subplots()
    fig.set_size_inches(20, 5)
    min_s = min(res1["qps"]["time"][0], res2["qps"]["time"][0])
    max_s = max(res1["qps"]["time"][-1], res2["qps"]["time"][-1])

    ax.stairs(res1["qps"]["counts"], res1["qps"]["time"], label="Baseline", alpha=0.5);
    ax.stairs(res2["qps"]["counts"], res2["qps"]["time"], label="Comparison", alpha=0.5);
    ax.set_xlabel("Time (s)")
    ax.set_ylabel("QPS")
    ax.legend()
    plt.show()


def display_table(compared, value, metric, orderby, top, relative):
    if relative:
        compared.sort(key=lambda x: orderby * ((x[2][value][metric]) / (x[0][value][metric]) if x[0][value][metric] != 0 else 0))
    else:
        compared.sort(key=lambda x: orderby * x[2][value][metric])
    table = f"""
|ID|Count|Change|Percentage|Baseline|Comparison|Errors|SQL|
|--|-----|------|----------|--------|----------|------|---|
"""
    for lhs, rhs, diff in compared[0:top]:
        table += f"|{lhs['id'] if 'id' in lhs else sha1(lhs['sql']).hexdigest().encode()}"
        table += f"|{lhs[value]['count']}"
        table += f"|{diff[value][metric]}"
        table += f"|{math.floor(10000 * (diff[value][metric]) / (lhs[value][metric])) / 100 if lhs[value][metric] != 0 else 0.0}%"
        table += f"|{lhs[value][metric]}"
        table += f"|{rhs[value][metric]}"
        table += f"|{diff['errors']}"
        table += f"|```{lhs['sql']}```"
        table += f"|\n"
    display(Markdown(table))


def plot_query_table(compared):
    value_w = widgets.Dropdown(options=["duration", "rows_read"], value="duration")
    metric_w = widgets.Dropdown(options=["sum", "mean", "min", "max", "stddev"], value="sum")
    orderby_w = widgets.Dropdown(options=[("Improved", 1), ("Degraded", -1)], value=1)
    relative_w = widgets.Dropdown(options=[("Relative", True), ("Absolute", False)], value=True)

    top_w = widgets.IntSlider(value=10, min=1, max=min(len(compared), 100), step=1,
                              continuous_update=False, orientation='horizontal',
                              readout=True, readout_format='d')

    out = widgets.interactive_output(display_table, {
        'value': value_w, 'metric': metric_w, 'orderby': orderby_w,
        'top': top_w, 'relative': relative_w, 'compared': widgets.fixed(compared)})
    box = widgets.HBox([widgets.Label("Value: "), value_w,
                        widgets.Label("Metric: "), metric_w,
                        widgets.Label("Order By: "), orderby_w,
                        widgets.Label("Change: "), relative_w,
                        widgets.Label("Top: "), top_w])

    display(box)
    display(out)


def show_histogram(compared, sql):
    lhs, rhs = [(v[0]["duration"], v[1]["duration"]) for v in compared if v[0]["sql"] == sql][0]
    fig, ax = plt.subplots()
    fig.set_size_inches(8, 8)
    ax.stairs(lhs["hist_counts"], lhs["hist_bins"], label="Baseline", alpha=0.5, fill=True)
    ax.stairs(rhs["hist_counts"], rhs["hist_bins"], label="Comparison", alpha=0.5, fill=True)
    ax.set_xlabel("Time (s)")
    ax.legend()


def plot_histogram(compared):
    id_list = [q[0]["sql"] for q in compared]
    id_list.sort()
    # Using `interactive` avoids having the window jump around when toggling the ID
    ia = widgets.interactive(show_histogram, sql = widgets.Dropdown(options=id_list, value=id_list[0]),
                             compared = widgets.fixed(compared))
    ia.layout.height = '1000px'
    display(ia)


def run():
    compared, res1, res2 = load_data()
    display(Markdown("# Queries Per Second"))
    plot_qps(res1, res2)
    display(Markdown("# Query Summaries"))
    plot_query_table(compared)
    display(Markdown("# Latency Histograms"))
    plot_histogram(compared)
