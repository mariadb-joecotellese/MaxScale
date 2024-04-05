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


def plot_one_qps(ax, res, label):
    t = res["qps"]["time"]
    c = res["qps"]["counts"]
    if len(t) > len(c) + 1:
        t = t[:len(c) + 1]
    elif len(c) >= len(t):
        c = c[:len(t) - 1]
    ax.stairs(c, t, label=label, alpha=0.5);


def plot_qps(res1, res2):
    fig, ax = plt.subplots()
    fig.set_size_inches(20, 5)
    plot_one_qps(ax, res1, "Baseline")
    plot_one_qps(ax, res2, "Comparison")
    ax.set_xlabel("Time (s)")
    ax.set_ylabel("QPS")
    ax.legend()
    plt.show()


def display_table(compared, value, metric, orderby, top, relative):
    if value == "errors":
        compared.sort(key=lambda x: orderby * x[2]["errors"])
    elif relative:
        compared.sort(key=lambda x: orderby * ((x[2][value][metric]) / (x[0][value][metric]) if x[0][value][metric] != 0 else 0))
    else:
        compared.sort(key=lambda x: orderby * x[2][value][metric])
    table = f"""
|ID|Count|Change|Percentage|Baseline|Comparison|Errors|SQL|
|--|-----|------|----------|--------|----------|------|---|
"""
    for lhs, rhs, diff in compared[0:top]:
        if value == "errors":
            dv = diff["errors"]
            lv = lhs["errors"]
            rv = rhs["errors"]
        else:
            dv = diff[value][metric]
            lv = lhs[value][metric]
            rv = rhs[value][metric]
        table += f"|{lhs['id'] if 'id' in lhs else sha1(lhs['sql']).hexdigest().encode()}"
        table += f"|{lhs['duration']['count']}"
        table += f"|{dv}"
        table += f"|{math.floor(10000 * (dv) / (lv)) / 100 if lv != 0 else 0.0}%"
        table += f"|{lv}"
        table += f"|{rv}"
        table += f"|{diff['errors']}"
        table += f"|```{lhs['sql']}```"
        table += f"|\n"
    display(Markdown(table))


def plot_query_table(compared):
    value_w = widgets.Dropdown(options=["duration", "rows_read", "errors"], value="duration")
    metric_w = widgets.Dropdown(options=["sum", "mean", "min", "max", "stddev"], value="sum")
    orderby_w = widgets.Dropdown(options=[("Improved", 1), ("Degraded", -1)], value=1)
    relative_w = widgets.Dropdown(options=[("Relative", True), ("Absolute", False)], value=True)

    def value_changed(changed):
        disable_on_error = changed["new"] == "errors"
        metric_w.disabled = disable_on_error
        relative_w.disabled = disable_on_error

    value_w.observe(value_changed, "value")

    top_w = widgets.IntSlider(value=10, min=1, max=min(len(compared), 100), step=1,
                              continuous_update=False, orientation='horizontal',
                              readout=True, readout_format='d')

    out = widgets.interactive_output(display_table, {
        'value': value_w, 'metric': metric_w, 'orderby': orderby_w,
        'top': top_w, 'relative': relative_w, 'compared': widgets.fixed(compared)})
    box = widgets.HBox([widgets.Label("Value: "), value_w,
                        widgets.Label("Order By: "), orderby_w,
                        widgets.Label("Metric: "), metric_w,
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
    id_list = [(f"{q[0]["id"]}: {q[0]["sql"][:100]}", q[0]["sql"]) for q in sorted(compared, key=lambda x: x[0]["id"])]
    # Using `interactive` avoids having the window jump around when toggling the ID
    ia = widgets.interactive(show_histogram, sql = widgets.Dropdown(options=id_list, value=id_list[0][1]),
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
