#
# Copyright (c) 2024 MariaDB plc
#
import matplotlib.pyplot as plt
from os import environ as env
import maxpostprocess as pp
import math
import json
import copy
from hashlib import sha1
from IPython.display import Markdown, HTML, display
import ipywidgets as widgets
from datetime import datetime

SORT_ASC = 1
SORT_DESC = -1
SORT_ID = 0
SHORT_SQL_LEN = 150
MAX_SQL_LEN = 400


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


def get_id_list(compared):
    """
    Convert the combined summary into an SQL ID list
    Args:
        compared: The combination of two summaries generated by maxpostprocess.py

    Returns: A list of typles with the first value as the field description and the second one as the field identifier
    """
    return [(f"{q[0]['id']}: {q[0]['sql'][:SHORT_SQL_LEN]}", q[0]["sql"]) for q in sorted(compared, key=lambda x: x[0]["id"])]


def markdown_widget(md):
    """
    Render markdown as a ipywidgets widget
    Args:
        md: The markdown to display

    Returns: The IPython Markdown as a ipywidgets widget
    """
    out = widgets.Output()
    with out:
        display(Markdown(md))
    return out


def accordion_widget(w, title):
    return widgets.Accordion(children=[w], titles=(title,))


def plot_one_qps(ax, res, label):
    t = [datetime.fromtimestamp(t) for t in res["qps"]["time"]]
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
    if orderby == SORT_ID:
        compared.sort(key=lambda x: x[0]["id"])
    elif value == "errors":
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
        table += f"|{'+' if diff['errors'] > 0 else ''}{diff['errors']}"
        table += f"|```{lhs['sql'] if len(lhs['sql']) < MAX_SQL_LEN else (lhs['sql'][0:MAX_SQL_LEN] + '...')}```"
        table += f"|\n"
    display(Markdown(table))


def plot_query_table(compared):
    value_w = widgets.Dropdown(options=["duration", "rows_read", "result_rows", "errors"], value="duration")
    metric_w = widgets.Dropdown(options=["sum", "mean", "min", "max", "stddev"], value="sum")
    orderby_w = widgets.Dropdown(options=[("Improved", SORT_ASC), ("Degraded", SORT_DESC), ("ID", SORT_ID)], value=1)
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
    box = widgets.HBox([widgets.Label("Order By: "), orderby_w,
                        widgets.Label("Value: "), value_w,
                        widgets.Label("Metric: "), metric_w,
                        widgets.Label("Change: "), relative_w,
                        widgets.Label("Top: "), top_w])

    display(box)
    display(out)


def show_one_histogram(ax, label, value, exclude_last, exclude_first):
    counts = value["hist_bin_counts"]
    if exclude_last or exclude_first:
        counts = copy.deepcopy(counts)
        if exclude_last:
            counts[-1] = 0
        if exclude_first:
            counts[0] = 0

    ax.stairs(counts, value["hist_bin_edges"], label=label, alpha=0.5, fill=True)


def show_one_explain(e):
    info_md = f"""
|Timestamp|Duration|
|---------|--------|
|{e['when']}|{e['duration']}|
"""
    sql_md = f"""
```
    {e['sql']}
```
"""
    json_md = f"""
```
    {json.dumps(e['json'], indent=2)}
```
"""

    info_w = markdown_widget(info_md)
    sql_w = accordion_widget(markdown_widget(sql_md), f"SQL")
    explain_w = accordion_widget(markdown_widget(json_md), "Explain")
    return widgets.VBox([info_w, sql_w, explain_w])


def make_explain_tab(explains):
    tab = widgets.Tab()
    tab.children = [show_one_explain(e) for e in explains]
    tab.titles = [str(i) for i in range(1, len(tab.children) + 1)]
    return tab


def show_explain(compared, sql):
    lhs, rhs = [(v[0]["explain"], v[1]["explain"]) for v in compared if v[0]["sql"] == sql][0]
    if len(lhs) == 0 or len(rhs) == 0:
        display(Markdown("**No explain for this query.**"))
    else:
        # Sort the values with slower queries first
        lhs.sort(key=lambda x: -x["duration"])
        rhs.sort(key=lambda x: -x["duration"])

        display(Markdown("## Baseline"))
        display(make_explain_tab(lhs))
        display(Markdown("## Comparison"))
        display(make_explain_tab(rhs))


def show_histogram(compared, sql, exclude_last, exclude_first):
    lhs, rhs = [(v[0]["duration"], v[1]["duration"]) for v in compared if v[0]["sql"] == sql][0]
    fig, ax = plt.subplots()
    fig.set_size_inches(8, 8)
    show_one_histogram(ax, "Baseline", lhs, exclude_last, exclude_first)
    show_one_histogram(ax, "Comparison", rhs, exclude_last, exclude_first)
    ax.set_xlabel("Time (s)")
    ax.legend()
    plt.show(fig) # This is needed so that widgets.Output() is able to capture it


def show_query_details(compared, sql):
    out_hist = widgets.Output()
    with out_hist:
        exclude_last_w = widgets.Checkbox(value=False, description="Exclude large outliers")
        exclude_first_w = widgets.Checkbox(value=False, description="Exclude small outliers")
        controls_w = widgets.HBox([exclude_first_w, exclude_last_w])
        io_w = widgets.interactive_output(show_histogram,
                                          {"sql": widgets.fixed(sql),
                                           "compared": widgets.fixed(compared),
                                           "exclude_last": exclude_last_w,
                                           "exclude_first": exclude_first_w
                                          })
        display(controls_w, io_w)

    out_explain = widgets.Output()
    with out_explain:
        try:
            show_explain(compared, sql)
        except KeyError:
            display(Markdown("No explains available."))

    tab = widgets.Tab()
    tab.children = [out_hist, out_explain]
    tab.titles = ["Latency Histograms", "Query Explains"]
    display(tab)


def plot_query_details(compared):
    id_list = get_id_list(compared)
    ia = widgets.interactive(show_query_details,
                             sql=widgets.Dropdown(options=id_list, value=id_list[0][1]),
                             compared=widgets.fixed(compared))
    ia.layout.height = '1000px'
    display(ia)


def run():
    compared, res1, res2 = load_data()
    display(Markdown("# Queries Per Second"))
    plot_qps(res1, res2)
    display(Markdown("# Query Summaries"))
    plot_query_table(compared)
    display(Markdown("# Query Details"))
    plot_query_details(compared)
