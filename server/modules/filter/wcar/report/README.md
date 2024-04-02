# WCAR Visualization

The `maxvisualize` program can be used to visualize the summaries of both WCAR
and DIFF reports.

## Processing WCAR Replays

If you are visualizing DIFF results, this section can be skipped.

The `maxpostprocess` program can be used to process the raw WCAR replay data
into a JSON summary file. The summary files are a lot smaller than the capture
and replay data which is why the usual workflow is to do the replay of the
capture, process the replay data into summaries and then do the visualization
locally.

The `.cx` capture file must be present on the same system where the CSV replay
file is.

The first step is to generate the CSV file with the query digests using the `maxplayer` program.

```
maxplayer list-queries /path/to/capture.cx -o canonicals.csv
```

After that, each replay can be processed into a summary.

```
maxpostprocess canonicals.csv /path/to/replay.csv -o summary.json
```

## Visualization

The visualization is done with the `maxvisualize` program. The first argument is
the path to the baseline summary file and the second argument is the path to the
comparison summary file.

```
maxvisualize /path/to/baseline-summary.json /path/to/comparison-summary.json
```

## Dependencies

Both `maxvisualize` and `maxpostprocess` use Python virtual environments
that are stored in `~/.maxvisualize/` and `~/.maxpostprocess/`
respectively. The environments are automatically updated whenever
MaxScale itself is updated.
